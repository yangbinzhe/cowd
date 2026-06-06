//! Runtime control policy and deterministic task complexity profiling.
//!
//! This module is intentionally small: it centralizes the first layer of
//! runtime decisions without introducing a scheduler or model dependency.

use serde::{Deserialize, Serialize};

use crate::context_runtime::ContextProfile;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplexityLevel {
    Simple,
    Focused,
    Complex,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentMode {
    Off,
    Assist,
    Parallel,
    ReviewOnly,
    CriticalSwarm,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplexitySignal {
    pub name: String,
    pub weight: u16,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskComplexityProfile {
    pub level: ComplexityLevel,
    pub score: u16,
    pub signals: Vec<ComplexitySignal>,
    pub recommended_profile: ContextProfile,
    pub recommended_agent_mode: AgentMode,
    pub requires_review: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplexityThresholds {
    pub simple_max: u16,
    pub focused_max: u16,
    pub complex_max: u16,
    pub critical_min: u16,
}

impl Default for ComplexityThresholds {
    fn default() -> Self {
        Self {
            simple_max: 24,
            focused_max: 49,
            complex_max: 79,
            critical_min: 80,
        }
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
    pub thresholds: ComplexityThresholds,
}

impl Default for TaskControlPolicy {
    fn default() -> Self {
        Self {
            auto_phase_for_yolo: true,
            review_after_each_phase: true,
            max_failures_before_review: 2,
            thresholds: ComplexityThresholds::default(),
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
            yolo_budget_tokens: 12_000,
            collaboration_budget_tokens: 10_000,
            review_budget_tokens: 9_000,
            degrade_on_pressure_bp: 8_500,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryControlPolicy {
    pub emit_pulses_from_workgraph: bool,
    pub review_conflicts: bool,
    pub max_candidates_per_turn: usize,
}

impl Default for MemoryControlPolicy {
    fn default() -> Self {
        Self {
            emit_pulses_from_workgraph: true,
            review_conflicts: true,
            max_candidates_per_turn: 8,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionControlPolicy {
    pub solo_honor_critical: bool,
    pub review_critical_actions: bool,
}

impl Default for PermissionControlPolicy {
    fn default() -> Self {
        Self {
            solo_honor_critical: true,
            review_critical_actions: true,
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
    pub permission: PermissionControlPolicy,
    pub observability: ObservabilityPolicy,
}

impl Default for RuntimeControlPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            agent: AgentControlPolicy::default(),
            task: TaskControlPolicy::default(),
            context: ContextControlPolicy::default(),
            memory: MemoryControlPolicy::default(),
            permission: PermissionControlPolicy::default(),
            observability: ObservabilityPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskComplexityInput {
    pub intent: String,
    pub current_profile: ContextProfile,
    pub yolo_mode: bool,
    pub prior_failures: u32,
    pub context_pressure_bp: u16,
}

impl TaskComplexityInput {
    #[must_use]
    pub fn new(intent: impl Into<String>, current_profile: ContextProfile) -> Self {
        let current_profile = current_profile;
        Self {
            intent: intent.into(),
            current_profile,
            yolo_mode: current_profile == ContextProfile::YoloGoal,
            prior_failures: 0,
            context_pressure_bp: 0,
        }
    }
}

impl RuntimeControlPolicy {
    #[must_use]
    pub fn profile_task(&self, input: &TaskComplexityInput) -> TaskComplexityProfile {
        let mut signals = Vec::new();
        let intent = input.intent.trim();
        let lower = intent.to_lowercase();
        let word_count = intent.split_whitespace().count();
        let segment_count = intent
            .split(|c: char| {
                c.is_ascii_punctuation() || c == '\n' || c == '，' || c == '。' || c == '；'
            })
            .filter(|segment| !segment.trim().is_empty())
            .count();

        if word_count >= 40 || intent.chars().count() >= 240 {
            signals.push(signal("large_intent", 16, "long user intent"));
        }
        if segment_count > 5 {
            signals.push(signal("multi_clause", 18, "many task clauses or steps"));
        }
        if contains_any(
            &lower,
            &[
                "multi-step",
                "parallel",
                "refactor",
                "migrate",
                "deploy",
                "analyze",
                "implement",
                "重构",
                "迁移",
                "部署",
                "分析",
                "实现",
            ],
        ) {
            signals.push(signal(
                "collaboration_trigger",
                36,
                "default collaboration trigger keyword",
            ));
        }
        if contains_any(
            &lower,
            &[
                "refactor",
                "restructure",
                "migrate",
                "implement",
                "architecture",
                "重构",
                "迁移",
                "实现",
                "架构",
            ],
        ) {
            signals.push(signal(
                "engineering_change",
                22,
                "implementation or architecture change",
            ));
        }
        if contains_any(
            &lower,
            &[
                "test",
                "coverage",
                "benchmark",
                "e2e",
                "playwright",
                "测试",
                "评测",
                "验收",
            ],
        ) {
            signals.push(signal(
                "verification_required",
                12,
                "explicit verification requirement",
            ));
        }
        if contains_any(
            &lower,
            &[
                "parallel",
                "multi-agent",
                "collaboration",
                "agent",
                "并行",
                "多agent",
                "协作",
            ],
        ) {
            signals.push(signal(
                "agent_collaboration",
                18,
                "explicit agent or parallel collaboration",
            ));
        }
        if contains_any(
            &lower,
            &[
                "security",
                "permission",
                "delete",
                "destructive",
                "critical",
                "安全",
                "权限",
                "删除",
                "危险",
            ],
        ) {
            signals.push(signal("risk_sensitive", 18, "risk-sensitive operation"));
        }
        if input.yolo_mode || input.current_profile == ContextProfile::YoloGoal {
            signals.push(signal("yolo_goal", 12, "continuous autonomous goal mode"));
        }
        if input.prior_failures >= self.task.max_failures_before_review {
            signals.push(signal("prior_failures", 20, "failure threshold reached"));
        }
        if input.context_pressure_bp >= self.context.degrade_on_pressure_bp {
            signals.push(signal("context_pressure", 10, "high context pressure"));
        }

        let score = signals
            .iter()
            .map(|signal| signal.weight)
            .sum::<u16>()
            .min(100);
        let thresholds = &self.task.thresholds;
        let level = if score >= thresholds.critical_min {
            ComplexityLevel::Critical
        } else if score > thresholds.focused_max {
            ComplexityLevel::Complex
        } else if score > thresholds.simple_max {
            ComplexityLevel::Focused
        } else {
            ComplexityLevel::Simple
        };

        let recommended_agent_mode = if !self.enabled || !self.agent.enabled {
            AgentMode::Off
        } else {
            match level {
                ComplexityLevel::Simple => AgentMode::Off,
                ComplexityLevel::Focused => AgentMode::Assist,
                ComplexityLevel::Complex => AgentMode::Parallel,
                ComplexityLevel::Critical => AgentMode::CriticalSwarm,
            }
        };
        let recommended_profile = match level {
            ComplexityLevel::Simple | ComplexityLevel::Focused => input.current_profile,
            ComplexityLevel::Complex => ContextProfile::Collaboration,
            ComplexityLevel::Critical => ContextProfile::Review,
        };
        let requires_review = matches!(level, ComplexityLevel::Critical)
            || signals.iter().any(|signal| signal.name == "risk_sensitive")
            || (self.task.review_after_each_phase
                && input.yolo_mode
                && matches!(level, ComplexityLevel::Complex));

        TaskComplexityProfile {
            level,
            score,
            signals,
            recommended_profile,
            recommended_agent_mode,
            requires_review,
        }
    }

    #[must_use]
    pub fn should_collaborate(&self, input: &TaskComplexityInput) -> bool {
        matches!(
            self.profile_task(input).recommended_agent_mode,
            AgentMode::Parallel | AgentMode::CriticalSwarm
        )
    }
}

fn signal(name: &str, weight: u16, reason: &str) -> ComplexitySignal {
    ComplexitySignal {
        name: name.to_string(),
        weight,
        reason: reason.to_string(),
    }
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_question_stays_on_main_turn_without_agents() {
        let policy = RuntimeControlPolicy::default();
        let profile = policy.profile_task(&TaskComplexityInput::new(
            "what is the current session id?",
            ContextProfile::MainTurn,
        ));

        assert_eq!(profile.level, ComplexityLevel::Simple);
        assert_eq!(profile.recommended_agent_mode, AgentMode::Off);
        assert_eq!(profile.recommended_profile, ContextProfile::MainTurn);
        assert!(!profile.requires_review);
    }

    #[test]
    fn engineering_task_recommends_parallel_collaboration() {
        let policy = RuntimeControlPolicy::default();
        let profile = policy.profile_task(&TaskComplexityInput::new(
            "refactor the runtime architecture, implement tests, run e2e validation, and update docs",
            ContextProfile::MainTurn,
        ));

        assert_eq!(profile.level, ComplexityLevel::Complex);
        assert_eq!(profile.recommended_agent_mode, AgentMode::Parallel);
        assert_eq!(profile.recommended_profile, ContextProfile::Collaboration);
        assert!(policy.should_collaborate(&TaskComplexityInput::new(
            "refactor the runtime architecture, implement tests, run e2e validation, and update docs",
            ContextProfile::MainTurn,
        )));
    }

    #[test]
    fn yolo_risky_failure_path_requires_review() {
        let policy = RuntimeControlPolicy::default();
        let mut input = TaskComplexityInput::new(
            "critical refactor migration: delete obsolete files, test security permissions, and verify rollout",
            ContextProfile::YoloGoal,
        );
        input.prior_failures = 2;

        let profile = policy.profile_task(&input);

        assert_eq!(profile.level, ComplexityLevel::Critical);
        assert_eq!(profile.recommended_agent_mode, AgentMode::CriticalSwarm);
        assert_eq!(profile.recommended_profile, ContextProfile::Review);
        assert!(profile.requires_review);
        assert!(
            profile
                .signals
                .iter()
                .any(|signal| signal.name == "risk_sensitive")
        );
    }

    #[test]
    fn disabled_agent_policy_never_collaborates() {
        let mut policy = RuntimeControlPolicy::default();
        policy.agent.enabled = false;
        let input = TaskComplexityInput::new(
            "refactor and implement tests across modules with parallel validation",
            ContextProfile::MainTurn,
        );

        assert!(!policy.should_collaborate(&input));
        assert_eq!(
            policy.profile_task(&input).recommended_agent_mode,
            AgentMode::Off
        );
    }
}
