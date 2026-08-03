//! Runtime control configuration.
//!
//! Task understanding and execution-pattern selection are owned by
//! `execution_core::StrategyDecisionEngine`. This module only carries tunable
//! resource, safety, memory, and observability policy.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentControlPolicy {
    pub enabled: bool,
    pub max_parallel_agents: usize,
    pub review_on_conflict: bool,
    pub require_positive_lift: bool,
    pub min_collaboration_score: u16,
}

impl Default for AgentControlPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_parallel_agents: 4,
            review_on_conflict: true,
            require_positive_lift: true,
            min_collaboration_score: 50,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskControlPolicy {
    pub auto_phase_for_yolo: bool,
    pub review_after_each_phase: bool,
    pub max_failures_before_review: u32,
}

impl Default for TaskControlPolicy {
    fn default() -> Self {
        Self {
            auto_phase_for_yolo: true,
            review_after_each_phase: true,
            max_failures_before_review: 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextControlPolicy {
    pub preserve_stable_head: bool,
    pub yolo_budget_tokens: u64,
    pub collaboration_budget_tokens: u64,
    pub review_budget_tokens: u64,
    pub degrade_on_pressure_bp: u16,
}

impl Default for ContextControlPolicy {
    fn default() -> Self {
        Self {
            preserve_stable_head: true,
            yolo_budget_tokens: 0,
            collaboration_budget_tokens: 0,
            review_budget_tokens: 0,
            degrade_on_pressure_bp: 8_500,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryControlPolicy {
    pub emit_pulses_from_execution_graph: bool,
    pub review_conflicts: bool,
    pub max_candidates_per_turn: usize,
}

impl Default for MemoryControlPolicy {
    fn default() -> Self {
        Self {
            emit_pulses_from_execution_graph: true,
            review_conflicts: true,
            max_candidates_per_turn: 8,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservabilityPolicy {
    pub emit_events: bool,
    pub explain: bool,
    pub webui: bool,
    pub tui: bool,
    pub debug_reasons: bool,
}

/// Runtime-owned policy for the Mission schedule timer event source. The
/// timer only submits durable SessionDispatch graphs; it never owns execution
/// state or advances graph nodes itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionSchedulePolicy {
    pub enabled: bool,
    pub tick_interval_ms: u64,
    pub grace_ms: u64,
}

impl Default for MissionSchedulePolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            tick_interval_ms: 1_000,
            grace_ms: 300_000,
        }
    }
}

impl MissionSchedulePolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.tick_interval_ms < 100 {
            return Err("mission schedule tick_interval_ms must be at least 100".to_string());
        }
        if self.grace_ms < self.tick_interval_ms {
            return Err("mission schedule grace_ms must be at least tick_interval_ms".to_string());
        }
        Ok(())
    }
}

impl Default for ObservabilityPolicy {
    fn default() -> Self {
        Self {
            emit_events: true,
            explain: true,
            webui: true,
            tui: true,
            debug_reasons: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeControlPolicy {
    pub enabled: bool,
    pub agent: AgentControlPolicy,
    pub task: TaskControlPolicy,
    pub context: ContextControlPolicy,
    pub memory: MemoryControlPolicy,
    pub observability: ObservabilityPolicy,
    pub mission_schedule: MissionSchedulePolicy,
}

impl Default for RuntimeControlPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            agent: AgentControlPolicy::default(),
            task: TaskControlPolicy::default(),
            context: ContextControlPolicy::default(),
            memory: MemoryControlPolicy::default(),
            observability: ObservabilityPolicy::default(),
            mission_schedule: MissionSchedulePolicy::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_contains_resources_without_task_classifier() {
        let policy = RuntimeControlPolicy::default();
        assert!(policy.agent.enabled);
        assert_eq!(policy.agent.max_parallel_agents, 4);
        assert_eq!(policy.task.max_failures_before_review, 2);
        assert_eq!(policy.context.degrade_on_pressure_bp, 8_500);
    }
}
