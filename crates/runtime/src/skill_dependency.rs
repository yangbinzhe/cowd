use serde::{Deserialize, Serialize};

use crate::iacc::IaccSkillManifest;

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

impl From<&IaccSkillManifest> for CowdSkillStructuredDependency {
    fn from(skill: &IaccSkillManifest) -> Self {
        Self {
            skill_id: format!("iacc:{}", skill.skill_id),
            domain: skill.domain.clone(),
            required_fact_types: skill
                .input_fact_types
                .iter()
                .map(|fact_type| format!("structured-fact-type:{fact_type}"))
                .collect(),
            required_metric_keys: skill.input_metric_keys.clone(),
            required_evidence: skill.required_evidence.clone(),
            quality_gate: skill.quality_gate.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iacc::server_manufacturing_skill_pack;

    #[test]
    fn skill_dependency_declares_structured_fact_types_and_quality_gate() {
        let skill = server_manufacturing_skill_pack()
            .into_iter()
            .find(|skill| skill.skill_id == "supply-risk-analyst")
            .expect("skill");

        let dependency = CowdSkillStructuredDependency::from(&skill);

        assert_eq!(dependency.skill_id, "iacc:supply-risk-analyst");
        assert!(dependency
            .required_fact_types
            .contains(&"structured-fact-type:supply.material_shortage".to_string()));
        assert_eq!(dependency.quality_gate, "evidence_quality_gate_required");
    }
}
