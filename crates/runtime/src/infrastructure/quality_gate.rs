use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::structured_data::CowdStructuredEvidence;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdStructuredQualityGate {
    pub gate_id: String,
    pub target_ref: String,
    pub decision: String,
    pub score: f32,
    #[serde(default)]
    pub structured_refs: Vec<String>,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub required_actions: Vec<String>,
    pub created_at: DateTime<Utc>,
}

impl CowdStructuredQualityGate {
    #[must_use]
    pub fn for_structured_evidence(evidence: &CowdStructuredEvidence) -> Self {
        let mut reasons = Vec::new();
        let mut required_actions = Vec::new();
        let mut score = evidence.confidence * 0.5;
        if evidence.source_refs.is_empty() {
            reasons.push("missing_structured_source_refs".to_string());
            required_actions.push("attach_structured_source_refs".to_string());
        } else {
            score += 0.25;
            reasons.push("structured_source_refs_present".to_string());
        }
        if evidence.metric_evidence.is_empty() {
            reasons.push("missing_metric_evidence".to_string());
            required_actions.push("collect_metric_evidence".to_string());
        } else {
            score += 0.25;
            reasons.push("metric_evidence_present".to_string());
        }
        let score = score.min(1.0);
        let decision = if score >= 0.75 {
            "pass"
        } else if score >= 0.45 {
            "review"
        } else {
            "fail"
        };

        Self {
            gate_id: format!("quality-gate:{}", evidence.evidence_id),
            target_ref: evidence.stable_ref(),
            decision: decision.to_string(),
            score,
            structured_refs: evidence
                .source_refs
                .iter()
                .map(|reference| reference.reference.clone())
                .collect(),
            reasons,
            required_actions,
            created_at: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structured_data::CowdStructuredEvidenceSourceRef;

    #[test]
    fn structured_quality_gate_uses_structured_evidence_refs() {
        let evidence = CowdStructuredEvidence {
            evidence_id: "packet-1".to_string(),
            attention_id: None,
            problem_statement: "shortage risk changed".to_string(),
            domain: Some("manufacturing".to_string()),
            business_context: serde_json::json!({}),
            metric_evidence: vec![serde_json::json!({"metric": "material_shortage_risk"})],
            change_evidence: Vec::new(),
            anomaly_evidence: Vec::new(),
            attribution_candidates: Vec::new(),
            impact_paths: Vec::new(),
            source_refs: vec![CowdStructuredEvidenceSourceRef {
                kind: "fact".to_string(),
                reference: "structured-fact:fact-1".to_string(),
                summary: "shortage fact".to_string(),
            }],
            missing_evidence: Vec::new(),
            confidence: 0.8,
            token_budget: 4096,
            created_at: DateTime::<Utc>::UNIX_EPOCH,
        };

        let gate = CowdStructuredQualityGate::for_structured_evidence(&evidence);

        assert_eq!(gate.target_ref, "structured-evidence:packet-1");
        assert_eq!(gate.decision, "pass");
        assert!(gate
            .structured_refs
            .contains(&"structured-fact:fact-1".to_string()));
        assert!(gate
            .reasons
            .contains(&"metric_evidence_present".to_string()));
    }
}
