//! Pure contracts for model-assisted evolution analysis.
//!
//! Model output is always a hypothesis-bearing Draft. These contracts carry
//! no release authority, executable Definition, Skill activation, or code
//! mutation capability.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const EVOLUTION_ANALYSIS_CONTRACT_VERSION: &str = "evolution-analysis-draft/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionAnalysisCandidateKind {
    AgentDefinition,
    TeamTemplate,
    Strategy,
    Skill,
    Tool,
    Connector,
    Runtime,
    Surface,
    CodePatch,
    ArchitecturePlan,
    TestScenario,
    Contract,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionAnalysisHypothesis {
    pub hypothesis_id: String,
    pub statement: String,
    pub supporting_evidence_refs: Vec<String>,
    pub contradicting_evidence_refs: Vec<String>,
    pub uncertainty: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionFalsificationExperiment {
    pub target_hypothesis_id: String,
    pub objective: String,
    pub method: Vec<String>,
    pub pass_criterion: String,
    pub falsification_criterion: String,
    pub required_evidence_refs: Vec<String>,
}

/// The only JSON shape a Provider is allowed to return.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionAnalysisModelOutput {
    pub hypotheses: Vec<EvolutionAnalysisHypothesis>,
    pub falsification_experiment: EvolutionFalsificationExperiment,
    pub suggested_candidate_kind: EvolutionAnalysisCandidateKind,
    pub acceptance_scenarios: Vec<String>,
    pub expected_value: String,
    pub estimated_cost: String,
    pub risks: Vec<String>,
    pub unknowns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionAnalysisUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub estimated_cost_microusd: Option<u64>,
    pub pricing_observed: bool,
    pub stop_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionAnalysisDraft {
    pub analysis_id: String,
    pub case_id: String,
    pub case_digest: String,
    pub contract_digest: String,
    pub input_digest: String,
    pub output_digest: String,
    pub provider: String,
    pub model: String,
    pub request_id: Option<String>,
    pub evidence_refs: Vec<String>,
    pub output: EvolutionAnalysisModelOutput,
    pub usage: EvolutionAnalysisUsage,
    pub created_at_ms: u64,
}

impl EvolutionAnalysisModelOutput {
    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(bytes))
    }
}

impl EvolutionAnalysisDraft {
    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(bytes))
    }
}

#[must_use]
pub fn evolution_analysis_contract_digest() -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(EVOLUTION_ANALYSIS_CONTRACT_VERSION.as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_output_contract_rejects_unknown_fields_and_has_stable_digest() {
        let value = serde_json::json!({
            "hypotheses": [{
                "hypothesis_id": "h1",
                "statement": "hypothesis",
                "supporting_evidence_refs": ["observed:test:e1"],
                "contradicting_evidence_refs": ["observed:test:e2"],
                "uncertainty": "unknown"
            }],
            "falsification_experiment": {
                "target_hypothesis_id": "h1",
                "objective": "test",
                "method": ["run"],
                "pass_criterion": "pass",
                "falsification_criterion": "fail",
                "required_evidence_refs": ["observed:test:e1"]
            },
            "suggested_candidate_kind": "architecture_plan",
            "acceptance_scenarios": ["scenario"],
            "expected_value": "value",
            "estimated_cost": "cost",
            "risks": ["risk"],
            "unknowns": ["unknown"]
        });
        let output: EvolutionAnalysisModelOutput =
            serde_json::from_value(value.clone()).expect("typed output");
        assert_eq!(output.digest(), output.digest());
        let mut unknown = value;
        unknown
            .as_object_mut()
            .expect("object")
            .insert("auto_publish".to_string(), serde_json::Value::Bool(true));
        assert!(serde_json::from_value::<EvolutionAnalysisModelOutput>(unknown).is_err());
    }
}
