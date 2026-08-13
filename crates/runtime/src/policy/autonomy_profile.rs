//! Autonomy profile strategy model.
//!
//! Autonomy profiles are pure policy contracts. They describe what the runtime
//! may do, when it must ask, and when it must escalate. They do not execute
//! tools or spawn agents.

use harness_contract::core::TaskRisk;
pub use harness_contract::policy::{AutonomyProfileId, InterruptionPolicy, SandboxPosture};
use serde::{Deserialize, Serialize};

use crate::{CollaborationTemplateId, PermissionMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalPolicy {
    AskAllWrites,
    AskRiskyWrites,
    AskCriticalOnly,
    DelegateLowRisk,
    TrustAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutonomyDecisionKind {
    Allow,
    RequireApproval,
    EscalateToHuman,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyBudget {
    pub max_parallelism: usize,
    pub max_turns: usize,
    pub max_tokens: u64,
    pub max_cost_cents: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyProfileSpec {
    pub profile_id: AutonomyProfileId,
    pub label: String,
    pub permission_mode: PermissionMode,
    pub approval_policy: ApprovalPolicy,
    pub interruption_policy: InterruptionPolicy,
    pub risk_threshold: TaskRisk,
    pub tool_scope: Vec<String>,
    pub budget: AutonomyBudget,
    pub reporting_cadence: String,
    pub human_escalation_rules: Vec<String>,
    pub compatible_collaboration_templates: Vec<CollaborationTemplateId>,
    pub sandbox_posture: SandboxPosture,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyDecisionInput {
    pub profile_id: AutonomyProfileId,
    pub requested_risk: TaskRisk,
    pub requested_tool: Option<String>,
    pub template_id: Option<CollaborationTemplateId>,
    pub requires_write: bool,
    pub is_critical_operation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyDecision {
    pub profile_id: AutonomyProfileId,
    pub decision: AutonomyDecisionKind,
    pub reason: String,
    pub evidence: Vec<String>,
    pub requested_risk: TaskRisk,
    pub policy_basis: Vec<String>,
    pub permission_mode: PermissionMode,
}

#[derive(Debug, Clone)]
pub struct AutonomyProfileCatalog {
    profiles: Vec<AutonomyProfileSpec>,
}

impl Default for AutonomyProfileCatalog {
    fn default() -> Self {
        Self {
            profiles: built_in_profiles(),
        }
    }
}

impl AutonomyProfileCatalog {
    #[must_use]
    pub fn built_in() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn profiles(&self) -> &[AutonomyProfileSpec] {
        &self.profiles
    }

    #[must_use]
    pub fn get(&self, profile_id: AutonomyProfileId) -> Option<&AutonomyProfileSpec> {
        self.profiles
            .iter()
            .find(|profile| profile.profile_id == profile_id)
    }

    #[must_use]
    pub fn decide(&self, input: AutonomyDecisionInput) -> AutonomyDecision {
        if let Some(profile) = self
            .get(input.profile_id)
            .or_else(|| self.get(AutonomyProfileId::Supervised))
        {
            return profile.decide(input);
        }
        AutonomyDecision {
            profile_id: input.profile_id,
            decision: AutonomyDecisionKind::Deny,
            reason: "autonomy profile catalog is empty".to_string(),
            evidence: vec!["autonomy_catalog_empty".to_string()],
            requested_risk: input.requested_risk,
            policy_basis: vec!["fail_closed".to_string()],
            permission_mode: PermissionMode::ReadOnly,
        }
    }
}

impl AutonomyProfileSpec {
    #[must_use]
    pub fn decide(&self, input: AutonomyDecisionInput) -> AutonomyDecision {
        let mut evidence = vec![
            format!("profile={}", self.profile_id.as_str()),
            format!("approval_policy={:?}", self.approval_policy),
            format!("risk_threshold={:?}", self.risk_threshold),
        ];
        let mut policy_basis = vec![
            format!("permission_mode={}", self.permission_mode.as_str()),
            format!("interruption_policy={:?}", self.interruption_policy),
            format!("max_parallelism={}", self.budget.max_parallelism),
        ];
        if let Some(template_id) = input.template_id {
            evidence.push(format!("template={}", template_id.as_str()));
            if !self
                .compatible_collaboration_templates
                .contains(&template_id)
            {
                return AutonomyDecision {
                    profile_id: self.profile_id,
                    decision: AutonomyDecisionKind::RequireApproval,
                    reason: format!(
                        "profile {} is not declared compatible with template {}",
                        self.profile_id.as_str(),
                        template_id.as_str()
                    ),
                    evidence,
                    requested_risk: input.requested_risk,
                    policy_basis,
                    permission_mode: self.permission_mode,
                };
            }
        }
        if let Some(tool) = &input.requested_tool {
            evidence.push(format!("tool={tool}"));
            if !self
                .tool_scope
                .iter()
                .any(|scope| scope == "*" || scope == tool)
            {
                return AutonomyDecision {
                    profile_id: self.profile_id,
                    decision: AutonomyDecisionKind::Deny,
                    reason: format!("tool `{tool}` is outside autonomy profile scope"),
                    evidence,
                    requested_risk: input.requested_risk,
                    policy_basis,
                    permission_mode: self.permission_mode,
                };
            }
        }
        if risk_exceeds(input.requested_risk, self.risk_threshold) {
            policy_basis.push("requested risk exceeds profile threshold".to_string());
            return AutonomyDecision {
                profile_id: self.profile_id,
                decision: AutonomyDecisionKind::EscalateToHuman,
                reason: "requested risk exceeds autonomy threshold".to_string(),
                evidence,
                requested_risk: input.requested_risk,
                policy_basis,
                permission_mode: self.permission_mode,
            };
        }
        let decision = match self.approval_policy {
            ApprovalPolicy::TrustAll => AutonomyDecisionKind::Allow,
            ApprovalPolicy::AskAllWrites if input.requires_write => {
                AutonomyDecisionKind::RequireApproval
            }
            ApprovalPolicy::AskRiskyWrites
                if input.requires_write && !matches!(input.requested_risk, TaskRisk::Low) =>
            {
                AutonomyDecisionKind::RequireApproval
            }
            ApprovalPolicy::AskCriticalOnly if input.is_critical_operation => {
                AutonomyDecisionKind::RequireApproval
            }
            ApprovalPolicy::DelegateLowRisk
                if matches!(input.requested_risk, TaskRisk::Low | TaskRisk::Medium) =>
            {
                AutonomyDecisionKind::Allow
            }
            _ => AutonomyDecisionKind::Allow,
        };
        let reason = match decision {
            AutonomyDecisionKind::Allow => "profile allows this action within scope",
            AutonomyDecisionKind::RequireApproval => "profile requires approval for this action",
            AutonomyDecisionKind::EscalateToHuman => "profile escalates this action",
            AutonomyDecisionKind::Deny => "profile denies this action",
        }
        .to_string();
        AutonomyDecision {
            profile_id: self.profile_id,
            decision,
            reason,
            evidence,
            requested_risk: input.requested_risk,
            policy_basis,
            permission_mode: self.permission_mode,
        }
    }
}

fn built_in_profiles() -> Vec<AutonomyProfileSpec> {
    vec![
        profile(
            AutonomyProfileId::Cautious,
            "Cautious",
            ApprovalPolicy::AskAllWrites,
            TaskRisk::Low,
            &["read_file", "grep_search", "glob_search"],
            AutonomyBudget {
                max_parallelism: 1,
                max_turns: 3,
                max_tokens: 8_000,
                max_cost_cents: Some(25),
            },
            "after every action",
            &["any write", "external side effect", "medium or higher risk"],
            &[
                CollaborationTemplateId::DirectExecutor,
                CollaborationTemplateId::DebateCriticArbiter,
            ],
        ),
        profile(
            AutonomyProfileId::Supervised,
            "Supervised",
            ApprovalPolicy::AskRiskyWrites,
            TaskRisk::Medium,
            &[
                "read_file",
                "grep_search",
                "glob_search",
                "write_file",
                "apply_patch",
                "bash",
            ],
            AutonomyBudget {
                max_parallelism: 2,
                max_turns: 10,
                max_tokens: 32_000,
                max_cost_cents: Some(150),
            },
            "at review and merge points",
            &[
                "high risk write",
                "destructive command",
                "external side effect",
            ],
            &[
                CollaborationTemplateId::DirectExecutor,
                CollaborationTemplateId::PlannerExecutorVerifier,
                CollaborationTemplateId::ImplementationReviewFix,
                CollaborationTemplateId::DebateCriticArbiter,
                CollaborationTemplateId::ParallelResearchSynthesis,
            ],
        ),
        profile(
            AutonomyProfileId::Autonomous,
            "Autonomous",
            ApprovalPolicy::AskCriticalOnly,
            TaskRisk::High,
            &["*"],
            AutonomyBudget {
                max_parallelism: 4,
                max_turns: 30,
                max_tokens: 96_000,
                max_cost_cents: Some(750),
            },
            "at milestone and critical decision points",
            &[
                "critical destructive command",
                "credential exposure",
                "release publish",
            ],
            &[
                CollaborationTemplateId::DirectExecutor,
                CollaborationTemplateId::PlannerExecutorVerifier,
                CollaborationTemplateId::ImplementationReviewFix,
                CollaborationTemplateId::DebateCriticArbiter,
                CollaborationTemplateId::ParallelResearchSynthesis,
                CollaborationTemplateId::LongRunningWorkstreams,
                CollaborationTemplateId::IncidentResponse,
            ],
        ),
        profile(
            AutonomyProfileId::Yolo,
            "Yolo",
            ApprovalPolicy::TrustAll,
            TaskRisk::Critical,
            &["*"],
            AutonomyBudget {
                max_parallelism: 4,
                max_turns: 40,
                max_tokens: 128_000,
                max_cost_cents: Some(1_000),
            },
            "continuous audit without interruption",
            &[
                "audit every action",
                "no human interruption",
            ],
            &[
                CollaborationTemplateId::DirectExecutor,
                CollaborationTemplateId::PlannerExecutorVerifier,
                CollaborationTemplateId::ImplementationReviewFix,
                CollaborationTemplateId::DebateCriticArbiter,
                CollaborationTemplateId::ParallelResearchSynthesis,
                CollaborationTemplateId::LongRunningWorkstreams,
                CollaborationTemplateId::IncidentResponse,
            ],
        ),
        profile(
            AutonomyProfileId::Stewarded,
            "Stewarded",
            ApprovalPolicy::DelegateLowRisk,
            TaskRisk::Medium,
            &[
                "read_file",
                "grep_search",
                "glob_search",
                "write_file",
                "apply_patch",
                "bash",
            ],
            AutonomyBudget {
                max_parallelism: 3,
                max_turns: 24,
                max_tokens: 64_000,
                max_cost_cents: Some(500),
            },
            "periodic steward report and every delegated approval",
            &["high risk action", "policy conflict", "budget pressure"],
            &[
                CollaborationTemplateId::DirectExecutor,
                CollaborationTemplateId::PlannerExecutorVerifier,
                CollaborationTemplateId::ImplementationReviewFix,
                CollaborationTemplateId::DebateCriticArbiter,
                CollaborationTemplateId::ParallelResearchSynthesis,
                CollaborationTemplateId::LongRunningWorkstreams,
            ],
        ),
    ]
}

fn profile(
    profile_id: AutonomyProfileId,
    label: &str,
    approval_policy: ApprovalPolicy,
    risk_threshold: TaskRisk,
    tool_scope: &[&str],
    budget: AutonomyBudget,
    reporting_cadence: &str,
    human_escalation_rules: &[&str],
    compatible_collaboration_templates: &[CollaborationTemplateId],
) -> AutonomyProfileSpec {
    let permission_mode = harness_contract::policy::permission_mode_for(profile_id);
    let interruption_policy = harness_contract::policy::interruption_policy_for(profile_id);
    let sandbox_posture = sandbox_posture_for(profile_id);
    AutonomyProfileSpec {
        profile_id,
        label: label.to_string(),
        permission_mode,
        approval_policy,
        interruption_policy,
        risk_threshold,
        tool_scope: tool_scope.iter().map(|item| (*item).to_string()).collect(),
        budget,
        reporting_cadence: reporting_cadence.to_string(),
        human_escalation_rules: human_escalation_rules
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
        compatible_collaboration_templates: compatible_collaboration_templates.to_vec(),
        sandbox_posture,
    }
}

pub fn sandbox_posture_for(profile_id: AutonomyProfileId) -> SandboxPosture {
    match profile_id {
        AutonomyProfileId::Cautious => SandboxPosture::ReadOnlySandbox,
        AutonomyProfileId::Supervised | AutonomyProfileId::Stewarded => {
            SandboxPosture::WorkspaceWriteSandbox
        }
        AutonomyProfileId::Autonomous | AutonomyProfileId::Yolo => SandboxPosture::HostFullAccess,
    }
}

fn risk_exceeds(requested: TaskRisk, threshold: TaskRisk) -> bool {
    risk_rank(requested) > risk_rank(threshold)
}

fn risk_rank(risk: TaskRisk) -> u8 {
    match risk {
        TaskRisk::Low => 0,
        TaskRisk::Medium => 1,
        TaskRisk::High => 2,
        TaskRisk::Critical => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_contains_expected_autonomy_profiles() {
        let catalog = AutonomyProfileCatalog::built_in();
        assert_eq!(catalog.profiles().len(), 5);
        assert!(catalog.get(AutonomyProfileId::Cautious).is_some());
        assert!(catalog.get(AutonomyProfileId::Supervised).is_some());
        assert!(catalog.get(AutonomyProfileId::Autonomous).is_some());
        assert!(catalog.get(AutonomyProfileId::Yolo).is_some());
        assert!(catalog.get(AutonomyProfileId::Stewarded).is_some());
        assert_eq!(
            catalog
                .get(AutonomyProfileId::Yolo)
                .expect("yolo profile")
                .budget
                .max_parallelism,
            4
        );
        assert_eq!(
            catalog
                .get(AutonomyProfileId::Autonomous)
                .expect("autonomous profile")
                .sandbox_posture,
            SandboxPosture::HostFullAccess
        );
    }

    #[test]
    fn supervised_requires_approval_for_risky_writes() {
        let decision = AutonomyProfileCatalog::built_in().decide(AutonomyDecisionInput {
            profile_id: AutonomyProfileId::Supervised,
            requested_risk: TaskRisk::Medium,
            requested_tool: Some("apply_patch".to_string()),
            template_id: Some(CollaborationTemplateId::ImplementationReviewFix),
            requires_write: true,
            is_critical_operation: false,
        });

        assert_eq!(decision.decision, AutonomyDecisionKind::RequireApproval);
        assert_eq!(decision.permission_mode, PermissionMode::WorkspaceWrite);
        assert!(decision.reason.contains("requires approval"));
    }

    #[test]
    fn yolo_trust_all_allows_critical_operations_without_escalation() {
        let decision = AutonomyProfileCatalog::built_in().decide(AutonomyDecisionInput {
            profile_id: AutonomyProfileId::Yolo,
            requested_risk: TaskRisk::Critical,
            requested_tool: Some("bash".to_string()),
            template_id: Some(CollaborationTemplateId::IncidentResponse),
            requires_write: true,
            is_critical_operation: true,
        });

        assert_eq!(decision.decision, AutonomyDecisionKind::Allow);
    }

    #[test]
    fn autonomous_escalates_critical_risk() {
        let decision = AutonomyProfileCatalog::built_in().decide(AutonomyDecisionInput {
            profile_id: AutonomyProfileId::Autonomous,
            requested_risk: TaskRisk::Critical,
            requested_tool: Some("bash".to_string()),
            template_id: Some(CollaborationTemplateId::IncidentResponse),
            requires_write: true,
            is_critical_operation: true,
        });

        assert_eq!(decision.decision, AutonomyDecisionKind::EscalateToHuman);
    }

    #[test]
    fn stewarded_delegates_low_risk_in_scope_actions() {
        let decision = AutonomyProfileCatalog::built_in().decide(AutonomyDecisionInput {
            profile_id: AutonomyProfileId::Stewarded,
            requested_risk: TaskRisk::Low,
            requested_tool: Some("read_file".to_string()),
            template_id: Some(CollaborationTemplateId::PlannerExecutorVerifier),
            requires_write: false,
            is_critical_operation: false,
        });

        assert_eq!(decision.decision, AutonomyDecisionKind::Allow);
        assert!(decision
            .evidence
            .iter()
            .any(|item| item == "tool=read_file"));
    }

    #[test]
    fn profile_denies_tools_outside_scope() {
        let decision = AutonomyProfileCatalog::built_in().decide(AutonomyDecisionInput {
            profile_id: AutonomyProfileId::Cautious,
            requested_risk: TaskRisk::Low,
            requested_tool: Some("bash".to_string()),
            template_id: Some(CollaborationTemplateId::DirectExecutor),
            requires_write: false,
            is_critical_operation: false,
        });

        assert_eq!(decision.decision, AutonomyDecisionKind::Deny);
    }
}
