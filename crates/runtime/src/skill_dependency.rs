use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn skill_dependency_declares_structured_fact_types_and_quality_gate() {
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
