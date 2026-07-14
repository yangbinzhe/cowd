//! Dynamic last-resort execution safety fuse.
//!
//! Goal acceptance and Runner synthesis decide business completion. This policy
//! only prevents an objectively stalled graph from growing without bound.

use harness_contract::core::TaskComplexity;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBudgetLease {
    pub base_max_model_steps: usize,
    pub max_model_steps: usize,
    pub context_window: u32,
    pub complexity: TaskComplexity,
    pub explicit_user_limit: Option<usize>,
    pub provider_tokens_per_second: Option<u32>,
    pub resource_pressure_basis_points: u16,
    pub last_novelty: u8,
    pub reason: String,
}

/// Measured Runtime facts used to refresh an already-issued safety lease.
/// These are local control signals, never a business completion decision.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SafetyFuseSignals {
    pub provider_tokens_per_second: Option<u32>,
    pub resource_pressure_basis_points: u16,
    pub novelty: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SafetyFuseDecision {
    Continue,
    Block { reason: String },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SafetyFusePolicy;

impl SafetyFusePolicy {
    #[must_use]
    pub fn derive(
        context_window: u32,
        complexity: TaskComplexity,
        explicit_user_limit: Option<usize>,
    ) -> ExecutionBudgetLease {
        let context_scale = usize::try_from(context_window.max(16_384) / 16_384)
            .unwrap_or(1)
            .clamp(1, 16);
        let complexity_multiplier = match complexity {
            TaskComplexity::Trivial => 1,
            TaskComplexity::Simple => 2,
            TaskComplexity::Moderate => 3,
            TaskComplexity::Complex => 5,
            TaskComplexity::Strategic => 8,
        };
        let derived = context_scale
            .saturating_mul(complexity_multiplier)
            .saturating_mul(4)
            .clamp(8, 512);
        let max_model_steps =
            explicit_user_limit.map_or(derived, |limit| derived.min(limit.max(1)));
        ExecutionBudgetLease {
            base_max_model_steps: max_model_steps,
            max_model_steps,
            context_window,
            complexity,
            explicit_user_limit,
            provider_tokens_per_second: None,
            resource_pressure_basis_points: 0,
            last_novelty: 0,
            reason:
                "derived from provider context window, task complexity, and explicit user limit"
                    .to_string(),
        }
    }

    /// Re-derive the remaining safety tolerance from measured provider speed,
    /// resource pressure, and novelty. Slow-but-progressing providers receive
    /// more opportunities; resource pressure and repeated low novelty reduce
    /// speculative retries. Explicit user limits remain an upper bound.
    #[must_use]
    pub fn refresh(
        lease: &ExecutionBudgetLease,
        signals: SafetyFuseSignals,
    ) -> ExecutionBudgetLease {
        let mut max_model_steps = lease.base_max_model_steps;
        if signals
            .provider_tokens_per_second
            .is_some_and(|tokens_per_second| tokens_per_second > 0 && tokens_per_second < 12)
        {
            max_model_steps = max_model_steps.saturating_add((max_model_steps / 4).max(1));
        }
        if signals.novelty >= 70 {
            max_model_steps = max_model_steps.saturating_add((max_model_steps / 8).max(1));
        }
        if signals.resource_pressure_basis_points >= 8_500 {
            max_model_steps = max_model_steps.saturating_mul(3).saturating_div(4).max(1);
        }
        if signals.novelty <= 15 {
            max_model_steps = max_model_steps.saturating_mul(4).saturating_div(5).max(1);
        }
        if let Some(limit) = lease.explicit_user_limit {
            max_model_steps = max_model_steps.min(limit.max(1));
        }
        ExecutionBudgetLease {
            max_model_steps: max_model_steps.clamp(1, 768),
            provider_tokens_per_second: signals.provider_tokens_per_second,
            resource_pressure_basis_points: signals.resource_pressure_basis_points,
            last_novelty: signals.novelty,
            reason: format!(
                "{}; refreshed from provider speed, resource pressure, and verified novelty",
                lease.reason
            ),
            ..lease.clone()
        }
    }

    #[must_use]
    pub fn evaluate(
        lease: &ExecutionBudgetLease,
        model_steps: usize,
        made_progress: bool,
    ) -> SafetyFuseDecision {
        if model_steps < lease.max_model_steps || made_progress {
            return SafetyFuseDecision::Continue;
        }
        SafetyFuseDecision::Block {
            reason: format!(
                "safety fuse exhausted after {model_steps} model steps without new goal progress; {}",
                lease.reason
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_from_context_and_complexity_not_prompt_classes() {
        let simple = SafetyFusePolicy::derive(128_000, TaskComplexity::Simple, None);
        let strategic = SafetyFusePolicy::derive(1_000_000, TaskComplexity::Strategic, None);
        assert!(strategic.max_model_steps > simple.max_model_steps);
        assert_eq!(
            SafetyFusePolicy::derive(1_000_000, TaskComplexity::Strategic, Some(3)).max_model_steps,
            3
        );
    }

    #[test]
    fn refresh_adapts_to_real_provider_and_progress_facts_without_escaping_user_limit() {
        let lease = SafetyFusePolicy::derive(128_000, TaskComplexity::Moderate, Some(20));
        let refreshed = SafetyFusePolicy::refresh(
            &lease,
            SafetyFuseSignals {
                provider_tokens_per_second: Some(6),
                resource_pressure_basis_points: 0,
                novelty: 90,
            },
        );
        assert!(refreshed.max_model_steps <= 20);
        assert_eq!(refreshed.provider_tokens_per_second, Some(6));
        let pressured = SafetyFusePolicy::refresh(
            &lease,
            SafetyFuseSignals {
                provider_tokens_per_second: Some(30),
                resource_pressure_basis_points: 9_000,
                novelty: 0,
            },
        );
        assert!(pressured.max_model_steps < lease.max_model_steps);
    }
}
