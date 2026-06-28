use serde::{Deserialize, Serialize};

use harness_contract::skill::SkillStructuredDependency;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdSkillStructuredDependency {
    pub skill_id: String,
    pub domain: String,
    #[serde(default)]
    pub required_fact_types: Vec<String>,
    #[serde(default)]
    pub required_metric_keys: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    pub quality_gate: String,
}

impl CowdSkillStructuredDependency {
    pub fn new(
        skill_id: impl Into<String>,
        domain: impl Into<String>,
        required_fact_types: Vec<String>,
        required_metric_keys: Vec<String>,
        required_evidence: Vec<String>,
        quality_gate: impl Into<String>,
    ) -> Self {
        Self {
            skill_id: skill_id.into(),
            domain: domain.into(),
            required_fact_types,
            required_metric_keys,
            required_evidence,
            quality_gate: quality_gate.into(),
        }
    }
}

impl CowdSkillStructuredDependency {
    #[must_use]
    pub fn from_contract(
        skill_id: impl Into<String>,
        dependency: &SkillStructuredDependency,
    ) -> Self {
        Self {
            skill_id: skill_id.into(),
            domain: dependency.domain.clone(),
            required_fact_types: dependency.required_fact_types.clone(),
            required_metric_keys: dependency.required_metric_keys.clone(),
            required_evidence: dependency.required_evidence.clone(),
            quality_gate: dependency.quality_gate.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn structured_dependency_declares_fact_types_and_quality_gate() {
        let dependency = CowdSkillStructuredDependency::new(
            "structured:supply-risk-analyst",
            "supply_chain",
            vec!["structured-fact-type:supply.material_shortage".to_string()],
            vec!["material_shortage_risk".to_string()],
            vec!["recent_supplier_evidence".to_string()],
            "evidence_quality_gate_required",
        );

        assert_eq!(dependency.skill_id, "structured:supply-risk-analyst");
        assert!(dependency
            .required_fact_types
            .contains(&"structured-fact-type:supply.material_shortage".to_string()));
        assert_eq!(dependency.quality_gate, "evidence_quality_gate_required");
    }
}
