use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixEvidenceSourceRef {
    pub kind: String,
    pub reference: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixEvidencePacket {
    pub packet_id: String,
    #[serde(default)]
    pub attention_id: Option<String>,
    pub problem_statement: String,
    #[serde(default)]
    pub business_context: Value,
    #[serde(default)]
    pub metric_evidence: Vec<Value>,
    #[serde(default)]
    pub change_evidence: Vec<Value>,
    #[serde(default)]
    pub anomaly_evidence: Vec<Value>,
    #[serde(default)]
    pub attribution_candidates: Vec<Value>,
    #[serde(default)]
    pub impact_paths: Vec<Value>,
    #[serde(default)]
    pub source_refs: Vec<MatrixEvidenceSourceRef>,
    #[serde(default)]
    pub missing_evidence: Vec<String>,
    pub confidence: f32,
    pub token_budget: u64,
    pub created_at: DateTime<Utc>,
}

impl MatrixEvidencePacket {
    #[must_use]
    pub fn new(problem_statement: impl Into<String>) -> Self {
        Self {
            packet_id: format!("evidence-{}", uuid::Uuid::new_v4()),
            attention_id: None,
            problem_statement: problem_statement.into(),
            business_context: Value::Null,
            metric_evidence: Vec::new(),
            change_evidence: Vec::new(),
            anomaly_evidence: Vec::new(),
            attribution_candidates: Vec::new(),
            impact_paths: Vec::new(),
            source_refs: Vec::new(),
            missing_evidence: vec![
                "attribution_not_computed_in_v0.9.79".to_string(),
                "impact_paths_not_computed_in_v0.9.79".to_string(),
            ],
            confidence: 0.3,
            token_budget: 4_000,
            created_at: Utc::now(),
        }
    }

    pub fn add_tool_invocation_source(
        &mut self,
        reference: &str,
        summary: &str,
        failure_kind: Option<&str>,
    ) {
        self.source_refs.push(MatrixEvidenceSourceRef {
            kind: "tool_invocation".to_string(),
            reference: reference.to_string(),
            summary: summary.to_string(),
        });
        if let Some(failure_kind) = failure_kind {
            self.anomaly_evidence.push(serde_json::json!({
                "kind": "tool_invocation_failure",
                "failure_kind": failure_kind,
                "source_ref": reference,
            }));
        }
        self.refresh_readiness();
    }

    pub fn add_agent_evidence_source(
        &mut self,
        node_id: &str,
        evidence_id: &str,
        evidence_kind: &str,
        reference: &str,
        summary: &str,
    ) {
        self.source_refs.push(MatrixEvidenceSourceRef {
            kind: "agent_evidence".to_string(),
            reference: reference.to_string(),
            summary: summary.to_string(),
        });
        self.attribution_candidates.push(serde_json::json!({
            "kind": "agent_evidence",
            "node_id": node_id,
            "evidence_id": evidence_id,
            "evidence_kind": evidence_kind,
            "source_ref": reference,
            "summary": summary,
        }));
        self.refresh_readiness();
    }

    fn refresh_readiness(&mut self) {
        self.missing_evidence.retain(|item| {
            !matches!(
                item.as_str(),
                "attribution_not_computed_in_v0.9.79" | "impact_paths_not_computed_in_v0.9.79"
            )
        });
        let typed_evidence_count = self.metric_evidence.len()
            + self.change_evidence.len()
            + self.anomaly_evidence.len()
            + self.attribution_candidates.len()
            + self.impact_paths.len();
        let source_score = (self.source_refs.len() as f32 * 0.05).min(0.20);
        let typed_score = (typed_evidence_count as f32 * 0.10).min(0.35);
        self.confidence = (0.30 + source_score + typed_score).min(0.85);
    }

    pub fn context_summary(&self) -> String {
        let metric_count = self.metric_evidence.len();
        let change_count = self.change_evidence.len();
        let missing = if self.missing_evidence.is_empty() {
            "none".to_string()
        } else {
            self.missing_evidence.join(", ")
        };
        format!(
            "MATRIX EvidencePacket {}: {}. metric_evidence={}, change_evidence={}, confidence={:.2}, missing_evidence={}",
            self.packet_id, self.problem_statement, metric_count, change_count, self.confidence, missing
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_packet_accepts_tool_invocation_source_without_output_copy() {
        let mut packet = MatrixEvidencePacket::new("supplier shortage risk");

        packet.add_tool_invocation_source(
            "tool-output:toolu-matrix:abc",
            "tool `bash`, status failed",
            Some("execution_error"),
        );

        assert_eq!(packet.source_refs.len(), 1);
        assert_eq!(packet.source_refs[0].kind, "tool_invocation");
        assert!(packet.source_refs[0].reference.starts_with("tool-output:"));
        assert_eq!(packet.anomaly_evidence.len(), 1);
        assert!(packet.confidence > 0.30);
        let serialized = serde_json::to_string(&packet).unwrap();
        assert!(!serialized.contains("unique-matrix-output-token-79"));
    }

    #[test]
    fn evidence_packet_accepts_agent_evidence_as_attribution_source() {
        let mut packet = MatrixEvidencePacket::new("bom explosion changed");

        packet.add_agent_evidence_source(
            "planner",
            "evidence-1",
            "tool_invocation",
            "tool-output:toolu-1:abc",
            "tool `bash`, status completed",
        );

        assert_eq!(packet.source_refs.len(), 1);
        assert_eq!(packet.source_refs[0].kind, "agent_evidence");
        assert_eq!(packet.attribution_candidates.len(), 1);
        assert!(packet.missing_evidence.is_empty());
        assert!(packet.confidence > 0.30);
    }
}
