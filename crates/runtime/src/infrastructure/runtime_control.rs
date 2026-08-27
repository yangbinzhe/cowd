//! Runtime control configuration.
//!
//! Task understanding and execution-pattern selection are owned by
//! `execution_core::StrategyDecisionEngine`. This module only carries tunable
//! resource, safety, memory, and observability policy.

use harness_contract::team::TeamExecutionCapacitySnapshot;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Absolute allocation guard, deliberately distinct from a deployable
/// collaboration limit.  Profiles above this value are rejected before any
/// Team graph allocation can happen.
pub const MAX_REPRESENTABLE_TEAM_AGENT_NODES: usize = 1_024;

/// Operator-owned collaboration and admission policy.  It contains every
/// normal execution bound that used to be scattered through compiler, Team,
/// approval and ResourceManager defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CollaborationCapacityPolicy {
    pub profile_id: String,
    pub revision: u64,
    pub max_program_teams: usize,
    pub max_team_roles: usize,
    pub max_role_instances_per_team: usize,
    pub max_agent_nodes_per_team: usize,
    pub max_pending_instance: usize,
    pub max_pending_per_class: usize,
    pub max_pending_per_key: usize,
    pub admission_aging_interval_ms: u64,
    pub user_team_veto_window_ms: u64,
    pub max_semantic_revisions_per_turn: usize,
}

impl Default for CollaborationCapacityPolicy {
    fn default() -> Self {
        Self {
            profile_id: "default-balanced".to_string(),
            revision: 1,
            max_program_teams: 32,
            max_team_roles: 32,
            max_role_instances_per_team: 32,
            max_agent_nodes_per_team: 32,
            max_pending_instance: 4_096,
            max_pending_per_class: 2_048,
            max_pending_per_key: 512,
            admission_aging_interval_ms: 5_000,
            user_team_veto_window_ms: 5_000,
            max_semantic_revisions_per_turn: 2,
        }
    }
}

impl CollaborationCapacityPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.profile_id.trim().is_empty()
            || self.revision == 0
            || self.max_program_teams == 0
            || self.max_team_roles == 0
            || self.max_role_instances_per_team == 0
            || self.max_agent_nodes_per_team == 0
            || self.max_agent_nodes_per_team > MAX_REPRESENTABLE_TEAM_AGENT_NODES
            || self.max_pending_instance == 0
            || self.max_pending_per_class == 0
            || self.max_pending_per_key == 0
            || self.max_pending_per_class > self.max_pending_instance
            || self.max_pending_per_key > self.max_pending_per_class
            || self.admission_aging_interval_ms == 0
            || self.user_team_veto_window_ms == 0
            || self.max_semantic_revisions_per_turn == 0
        {
            return Err("invalid collaboration capacity policy".to_string());
        }
        // The concrete Team validator checks the sum of its declared role
        // cardinalities against `max_agent_nodes_per_team`.  Multiplying two
        // independent maxima here would reject the locked defaults (32 roles,
        // each individually allowed up to 32) even though a real Team may use
        // only one instance per role.
        Ok(())
    }
}

/// Fully resolved operator profile used by one Runtime process. The Agent
/// width comes from the existing control policy, then every admission freezes
/// this value into a contract-owned Team snapshot and Program ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCapacityProfile {
    pub schema_version: u16,
    pub profile_id: String,
    pub revision: u64,
    pub digest: String,
    pub max_program_teams: usize,
    pub max_team_roles: usize,
    pub max_role_instances_per_team: usize,
    pub max_agent_nodes_per_team: usize,
    pub max_parallel_agents: usize,
    pub max_pending_instance: usize,
    pub max_pending_per_class: usize,
    pub max_pending_per_key: usize,
    pub admission_aging_interval_ms: u64,
    pub user_team_veto_window_ms: u64,
    pub max_semantic_revisions_per_turn: usize,
}

impl ExecutionCapacityProfile {
    pub fn resolve(
        policy: &CollaborationCapacityPolicy,
        max_parallel_agents: usize,
    ) -> Result<Self, String> {
        policy.validate()?;
        if max_parallel_agents == 0 {
            return Err("invalid configured Agent parallelism".to_string());
        }
        let mut profile = Self {
            schema_version: 1,
            profile_id: policy.profile_id.clone(),
            revision: policy.revision,
            digest: String::new(),
            max_program_teams: policy.max_program_teams,
            max_team_roles: policy.max_team_roles,
            max_role_instances_per_team: policy.max_role_instances_per_team,
            max_agent_nodes_per_team: policy.max_agent_nodes_per_team,
            max_parallel_agents,
            max_pending_instance: policy.max_pending_instance,
            max_pending_per_class: policy.max_pending_per_class,
            max_pending_per_key: policy.max_pending_per_key,
            admission_aging_interval_ms: policy.admission_aging_interval_ms,
            user_team_veto_window_ms: policy.user_team_veto_window_ms,
            max_semantic_revisions_per_turn: policy.max_semantic_revisions_per_turn,
        };
        profile.digest = profile.compute_digest();
        Ok(profile)
    }

    #[must_use]
    pub fn team_snapshot(&self) -> TeamExecutionCapacitySnapshot {
        TeamExecutionCapacitySnapshot {
            schema_version: self.schema_version,
            profile_id: self.profile_id.clone(),
            revision: self.revision,
            digest: self.digest.clone(),
            max_program_teams: self.max_program_teams,
            max_team_roles: self.max_team_roles,
            max_role_instances_per_team: self.max_role_instances_per_team,
            max_agent_nodes_per_team: self.max_agent_nodes_per_team,
            max_parallel_agents: self.max_parallel_agents,
        }
    }

    fn compute_digest(&self) -> String {
        let canonical = format!(
            "{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}",
            self.schema_version,
            self.profile_id,
            self.revision,
            self.max_program_teams,
            self.max_team_roles,
            self.max_role_instances_per_team,
            self.max_agent_nodes_per_team,
            self.max_parallel_agents,
            self.max_pending_instance,
            self.max_pending_per_class,
            self.max_pending_per_key,
            self.admission_aging_interval_ms,
            self.user_team_veto_window_ms,
            self.max_semantic_revisions_per_turn,
        );
        format!("sha256:{:x}", Sha256::digest(canonical.as_bytes()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentControlPolicy {
    pub enabled: bool,
    pub max_parallel_agents: usize,
    pub review_on_conflict: bool,
    pub require_positive_lift: bool,
    pub min_collaboration_score: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveExecutionCapacity {
    pub configured_agent_ceiling: usize,
    pub configured_tool_ceiling: usize,
    pub selected_agent_width: usize,
    pub selected_tool_width: usize,
    pub reason_codes: Vec<String>,
}

impl EffectiveExecutionCapacity {
    #[must_use]
    pub fn from_policy(policy: &RuntimeControlPolicy) -> Self {
        let agent = policy.agent.max_parallel_agents.max(1);
        let tools = crate::governed_tool_plan::default_parallel_tool_concurrency();
        Self {
            configured_agent_ceiling: agent,
            configured_tool_ceiling: tools,
            selected_agent_width: agent,
            selected_tool_width: tools,
            reason_codes: vec!["configured_ceiling".to_string()],
        }
    }
}

impl Default for AgentControlPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_parallel_agents: 42,
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
    #[serde(default)]
    pub capacity: CollaborationCapacityPolicy,
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
            capacity: CollaborationCapacityPolicy::default(),
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
        assert_eq!(policy.agent.max_parallel_agents, 42);
        let capacity = EffectiveExecutionCapacity::from_policy(&policy);
        assert_eq!(capacity.selected_agent_width, 42);
        assert_eq!(capacity.selected_tool_width, 42);
        assert_eq!(policy.task.max_failures_before_review, 2);
        assert_eq!(policy.context.degrade_on_pressure_bp, 8_500);
    }

    #[test]
    fn resolved_profile_binds_agent_width_into_a_stable_team_snapshot() {
        let policy = RuntimeControlPolicy::default();
        let profile = ExecutionCapacityProfile::resolve(&policy.capacity, 7)
            .expect("valid default capacity profile");
        let same_profile = ExecutionCapacityProfile::resolve(&policy.capacity, 7)
            .expect("same inputs resolve identically");

        assert_eq!(profile.digest, same_profile.digest);
        assert_eq!(profile.team_snapshot().max_parallel_agents, 7);
        assert_eq!(profile.team_snapshot().max_agent_nodes_per_team, 32);
    }

    #[test]
    fn capacity_policy_rejects_invalid_queue_relation() {
        let mut capacity = CollaborationCapacityPolicy::default();
        capacity.max_pending_per_key = capacity.max_pending_per_class + 1;
        assert!(capacity.validate().is_err());
    }
}
