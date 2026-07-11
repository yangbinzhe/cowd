use harness_contract::team::TeamLiftVerdict;

/// Inputs intentionally describe work shape rather than fixed role counts.
/// The graph builder receives the verdict and the runtime resource manager
/// enforces actual concurrency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollaborationLiftInput {
    pub independent_work_items: usize,
    pub domain_count: usize,
    pub shared_write_scope: bool,
    pub review_required: bool,
    pub provider_healthy: bool,
    pub budget_allows_parallelism: bool,
    pub requested_parallelism: usize,
}

#[derive(Debug, Default)]
pub struct CollaborationLiftGate;

impl CollaborationLiftGate {
    #[must_use]
    pub fn decide(&self, input: &CollaborationLiftInput) -> TeamLiftVerdict {
        let requested = input.requested_parallelism.max(1);
        let mut reasons = Vec::new();
        if !input.provider_healthy {
            reasons.push("provider health does not allow a parallel team".into());
        }
        if !input.budget_allows_parallelism {
            reasons.push("current budget does not allow coordination overhead".into());
        }
        if input.independent_work_items < 2 && !input.review_required {
            reasons.push("work has no independent fanout or review need".into());
        }
        if input.shared_write_scope && input.independent_work_items < 3 {
            reasons.push("shared write scope would serialize the proposed team".into());
        }
        let accepted = reasons.is_empty();
        let max_parallel_agents = if accepted {
            input
                .independent_work_items
                .min(requested)
                .min(input.domain_count.max(1))
                .max(1)
        } else {
            1
        };
        if accepted {
            reasons.push(format!(
                "{} independent work items can overlap with bounded coordination",
                input.independent_work_items
            ));
        }
        TeamLiftVerdict {
            accepted,
            max_parallel_agents,
            reasons,
            resized_from: requested,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_low_lift_work_without_team_overhead() {
        let verdict = CollaborationLiftGate.decide(&CollaborationLiftInput {
            independent_work_items: 1,
            domain_count: 1,
            shared_write_scope: false,
            review_required: false,
            provider_healthy: true,
            budget_allows_parallelism: true,
            requested_parallelism: 4,
        });
        assert!(!verdict.accepted);
        assert_eq!(verdict.max_parallel_agents, 1);
    }

    #[test]
    fn resizes_valid_fanout_to_real_independence() {
        let verdict = CollaborationLiftGate.decide(&CollaborationLiftInput {
            independent_work_items: 3,
            domain_count: 2,
            shared_write_scope: false,
            review_required: true,
            provider_healthy: true,
            budget_allows_parallelism: true,
            requested_parallelism: 6,
        });
        assert!(verdict.accepted);
        assert_eq!(verdict.max_parallel_agents, 2);
    }
}
