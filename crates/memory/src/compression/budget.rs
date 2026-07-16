//! Token budget management and read-depth scaling.
//!
//! Tracks how tokens are allocated across system prompt, memory, and
//! conversation history.  Implements depth-scale reduction when the budget
//! is under pressure.

use crate::{
    config::BudgetConfig,
    types::{AlertLevel, TokenBudget},
};

/// Which subsystem is requesting a budget allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationPhase {
    SystemPrompt,
    MemoryInjection,
    ConversationHistory,
    ToolOutput,
}

/// Token allocation for a specific phase.
#[derive(Debug, Clone, Copy)]
pub struct Allocation {
    pub max_tokens: u64,
    pub pressure_factor: f32,
}

/// Manages token allocation and emits budget snapshots.
pub struct BudgetManager {
    config: BudgetConfig,
}

impl BudgetManager {
    #[must_use]
    pub fn new(config: BudgetConfig) -> Self {
        Self { config }
    }

    /// Build an initial budget from the current configuration.
    #[must_use]
    pub fn initial_budget(&self) -> TokenBudget {
        let total = self.config.context_window;
        let reserved_system = self.config.reserved_system;
        let reserved_response = self.config.reserved_response;
        let available = total
            .saturating_sub(reserved_system)
            .saturating_sub(reserved_response);
        TokenBudget {
            total,
            reserved_system,
            reserved_response,
            allocated_memory: 0,
            allocated_conversation: 0,
            available,
        }
    }

    /// Compute the current alert level given actual token usage.
    #[must_use]
    pub fn alert_level(&self, used: u64) -> AlertLevel {
        let ratio = used as f32 / self.config.context_window as f32;
        if ratio >= self.config.critical_threshold {
            AlertLevel::Critical
        } else if ratio >= self.config.warning_threshold {
            AlertLevel::Warning
        } else {
            AlertLevel::Normal
        }
    }

    /// Compute a read-depth scale factor in `[0.0, 1.0]` based on pressure.
    ///
    /// Returns `1.0` (full depth) when usage is below the warning threshold,
    /// scaling linearly to `0.0` at the critical threshold.
    #[must_use]
    pub fn depth_scale(&self, used: u64) -> f32 {
        let ratio = used as f32 / self.config.context_window as f32;
        let warn = self.config.warning_threshold;
        let crit = self.config.critical_threshold;
        if ratio <= warn {
            1.0
        } else if ratio >= crit {
            0.0
        } else {
            1.0 - (ratio - warn) / (crit - warn)
        }
    }

    /// Dynamically allocate tokens for a given phase based on current pressure.
    /// Under high pressure (>80%), critical phases get proportionally reduced budgets.
    #[must_use]
    pub fn allocate(&self, phase: AllocationPhase, used: u64) -> Allocation {
        let total = self.config.context_window;
        let pressure = used as f32 / total as f32;

        let base_pct = match phase {
            AllocationPhase::SystemPrompt => 0.15,
            AllocationPhase::MemoryInjection => 0.12,
            AllocationPhase::ConversationHistory => 0.55,
            AllocationPhase::ToolOutput => 0.18,
        };

        let pressure_factor = if pressure > 0.8 {
            // High pressure: reduce non-critical phases
            match phase {
                AllocationPhase::SystemPrompt => 1.0,
                AllocationPhase::MemoryInjection => 0.5,
                AllocationPhase::ConversationHistory => 0.7,
                AllocationPhase::ToolOutput => 0.5,
            }
        } else {
            1.0
        };

        Allocation {
            max_tokens: ((total as f32) * base_pct * pressure_factor) as u64,
            pressure_factor,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a15_normal_pressure_full_allocation() {
        let mgr = BudgetManager::new(BudgetConfig {
            context_window: 100_000,
            reserved_system: 10_000,
            reserved_response: 4_000,
            warning_threshold: 0.7,
            critical_threshold: 0.95,
            ..Default::default()
        });
        let alloc = mgr.allocate(AllocationPhase::MemoryInjection, 50_000);
        assert!(alloc.max_tokens > 0);
        assert!((alloc.pressure_factor - 1.0).abs() < 0.01);
    }

    #[test]
    fn a15_high_pressure_reduces_memory() {
        let mgr = BudgetManager::new(BudgetConfig {
            context_window: 100_000,
            reserved_system: 10_000,
            reserved_response: 4_000,
            warning_threshold: 0.7,
            critical_threshold: 0.95,
            ..Default::default()
        });
        let alloc = mgr.allocate(AllocationPhase::MemoryInjection, 85_000);
        assert!(
            (alloc.pressure_factor - 0.5).abs() < 0.01,
            "memory phase should halve under high pressure, got {}",
            alloc.pressure_factor
        );
    }

    #[test]
    fn a15_system_prompt_never_reduced() {
        let mgr = BudgetManager::new(BudgetConfig {
            context_window: 100_000,
            reserved_system: 10_000,
            reserved_response: 4_000,
            warning_threshold: 0.7,
            critical_threshold: 0.95,
            ..Default::default()
        });
        let alloc = mgr.allocate(AllocationPhase::SystemPrompt, 90_000);
        assert!((alloc.pressure_factor - 1.0).abs() < 0.01);
    }
}
