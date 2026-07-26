//! Dynamic last-resort execution safety fuse.
//!
//! Goal acceptance and Runner synthesis decide business completion. This policy
//! bounds the amount of model work a graph may perform before it must publish
//! an honest terminal result. Progress influences intervention choice inside
//! the lease, but never turns a declared maximum into an advisory threshold.

use harness_contract::core::TaskComplexity;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBudgetLease {
    pub max_model_steps: usize,
    pub complexity: TaskComplexity,
    pub explicit_user_limit: Option<usize>,
    pub reason: String,
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
        let base_steps = match complexity {
            TaskComplexity::Trivial => 4,
            TaskComplexity::Simple => 8,
            TaskComplexity::Moderate => 14,
            TaskComplexity::Complex => 24,
            TaskComplexity::Strategic => 32,
        };
        let max_model_steps =
            explicit_user_limit.map_or(base_steps, |limit| base_steps.min(limit.max(1)));
        ExecutionBudgetLease {
            max_model_steps,
            complexity,
            explicit_user_limit,
            reason: format!(
                "hard safety ceiling derived from policy complexity={complexity:?}, explicit_user_limit={explicit_user_limit:?}; context_window={context_window} is context-budget telemetry only"
            ),
        }
    }

    #[must_use]
    pub fn evaluate(
        lease: &ExecutionBudgetLease,
        model_steps: usize,
        _made_progress: bool,
    ) -> SafetyFuseDecision {
        if model_steps < lease.max_model_steps {
            return SafetyFuseDecision::Continue;
        }
        SafetyFuseDecision::Block {
            reason: format!(
                "safety fuse exhausted after {model_steps} model steps at the absolute lease limit; {}",
                lease.reason
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_from_complexity_and_explicit_limit_not_context_window() {
        let simple = SafetyFusePolicy::derive(128_000, TaskComplexity::Simple, None);
        let strategic = SafetyFusePolicy::derive(1_000_000, TaskComplexity::Strategic, None);
        assert!(strategic.max_model_steps > simple.max_model_steps);
        assert_eq!(
            SafetyFusePolicy::derive(128_000, TaskComplexity::Strategic, None).max_model_steps,
            strategic.max_model_steps
        );
        assert_eq!(
            SafetyFusePolicy::derive(1_000_000, TaskComplexity::Strategic, Some(3)).max_model_steps,
            3
        );
    }

    #[test]
    fn hard_ceiling_is_fixed_for_the_lifetime_of_the_lease() {
        let lease = SafetyFusePolicy::derive(1_000_000, TaskComplexity::Moderate, Some(20));
        assert_eq!(lease.max_model_steps, 14);
        assert!(lease.reason.contains("context-budget telemetry only"));
    }

    #[test]
    fn reaching_the_lease_limit_blocks_even_when_the_latest_step_made_progress() {
        let lease = SafetyFusePolicy::derive(128_000, TaskComplexity::Complex, Some(3));
        assert_eq!(lease.max_model_steps, 3);
        assert_eq!(
            SafetyFusePolicy::evaluate(&lease, 2, true),
            SafetyFuseDecision::Continue
        );
        assert!(matches!(
            SafetyFusePolicy::evaluate(&lease, 3, true),
            SafetyFuseDecision::Block { .. }
        ));
    }
}
