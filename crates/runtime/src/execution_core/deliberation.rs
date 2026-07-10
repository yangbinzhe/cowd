use harness_contract::core::ExecutionPattern;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::collaboration_template::CollaborationTemplateId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliberationMode {
    DebateConsensus,
    JointProblemSolving,
    MultiPathSearch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliberationPlan {
    pub plan_id: String,
    pub objective: String,
    pub execution_pattern: ExecutionPattern,
    pub mode: DeliberationMode,
    pub template_hint: CollaborationTemplateId,
    pub candidate_count: usize,
    pub evaluation_contract: String,
}

impl DeliberationPlan {
    #[must_use]
    pub fn for_objective(objective: &str) -> Self {
        Self {
            plan_id: format!("deliberation-{}", Uuid::new_v4()),
            objective: objective.to_string(),
            execution_pattern: ExecutionPattern::Deliberate,
            mode: DeliberationMode::DebateConsensus,
            template_hint: CollaborationTemplateId::DebateConsensus,
            candidate_count: 3,
            evaluation_contract:
                "Generate competing options, critique assumptions, merge the strongest path, and list unresolved risks."
                    .to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deliberation_plan_uses_debate_consensus_template() {
        let plan = DeliberationPlan::for_objective("评估两种架构取舍");
        assert_eq!(plan.execution_pattern, ExecutionPattern::Deliberate);
        assert_eq!(plan.template_hint, CollaborationTemplateId::DebateConsensus);
        assert!(plan.evaluation_contract.contains("critique"));
    }
}
