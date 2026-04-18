//! Token budget management and read-depth scaling.
//!
//! Tracks how tokens are allocated across system prompt, memory, and
//! conversation history.  Implements depth-scale reduction when the budget
//! is under pressure.
//!
//! TODO: implement budget allocation algorithm.

use crate::{
    config::BudgetConfig,
    types::{AlertLevel, TokenBudget},
};

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
        let available = total.saturating_sub(reserved_system).saturating_sub(reserved_response);
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
}
