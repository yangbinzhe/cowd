use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::evidence_planner::{plan_evidence, EvidencePlan};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewooEvidencePlan {
    pub plan_id: String,
    pub objective: String,
    pub evidence_plan: EvidencePlan,
    pub steps: Vec<RewooEvidenceStep>,
    pub solver_contract: RewooSolverContract,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewooEvidenceStep {
    pub id: String,
    pub tool_name: String,
    pub input: Value,
    pub depends_on: Vec<String>,
    pub output_ref: String,
    pub purpose: String,
    pub max_output_tokens: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewooSolverContract {
    pub required_summary: String,
    pub missing_evidence_policy: String,
    pub answer_guidance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewooObservation {
    pub step_id: String,
    pub output_ref: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewooEvidenceResult {
    pub plan_id: String,
    pub observations: Vec<RewooObservation>,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub next_guidance: String,
}

#[must_use]
pub fn rewoo_plan_for_intent(intent: &str) -> RewooEvidencePlan {
    let evidence_plan = plan_evidence(intent);
    let steps = evidence_plan
        .recommended_calls
        .iter()
        .enumerate()
        .map(|(index, call)| RewooEvidenceStep {
            id: format!("E{}", index + 1),
            tool_name: call.name.clone(),
            input: call.input.clone(),
            depends_on: Vec::new(),
            output_ref: format!("evidence.{}", index + 1),
            purpose: format!("Gather checked evidence for `{}`", call.name),
            max_output_tokens: Some(6_000),
        })
        .collect::<Vec<_>>();

    RewooEvidencePlan {
        plan_id: format!("rewoo-{}", Uuid::new_v4()),
        objective: intent.to_string(),
        evidence_plan,
        steps,
        solver_contract: RewooSolverContract {
            required_summary: "Summarize checked facts, contradictions, and confidence.".to_string(),
            missing_evidence_policy:
                "If evidence is insufficient, state missing evidence and propose the lightest next action."
                    .to_string(),
            answer_guidance:
                "Use evidence refs and compact summaries; do not flood the conversation with raw outputs."
                    .to_string(),
        },
    }
}

impl RewooEvidencePlan {
    #[must_use]
    pub fn synthetic_result(&self) -> RewooEvidenceResult {
        let observations = self
            .steps
            .iter()
            .map(|step| RewooObservation {
                step_id: step.id.clone(),
                output_ref: step.output_ref.clone(),
                summary: format!("planned {} for {}", step.tool_name, step.purpose),
            })
            .collect::<Vec<_>>();
        RewooEvidenceResult {
            plan_id: self.plan_id.clone(),
            evidence_refs: self
                .steps
                .iter()
                .map(|step| step.output_ref.clone())
                .collect(),
            observations,
            summary: format!("{} evidence steps planned", self.steps.len()),
            next_guidance: self.solver_contract.answer_guidance.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewoo_plan_contains_variable_evidence_steps() {
        let plan = rewoo_plan_for_intent("检查 README 是否反映最新架构");
        assert!(!plan.steps.is_empty());
        assert!(plan
            .steps
            .iter()
            .all(|step| step.output_ref.starts_with("evidence.")));
        assert!(plan
            .solver_contract
            .answer_guidance
            .contains("evidence refs"));
    }
}
