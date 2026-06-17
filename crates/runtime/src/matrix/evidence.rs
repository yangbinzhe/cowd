use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agent_protocol::AgentEvidence;
use crate::tool_invocation::ToolInvocationRecord;
use crate::{ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextVisibility};

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

    #[must_use]
    pub fn to_context_item(&self) -> ContextItem {
        let mut item = ContextItem::new(
            format!("matrix:evidence:{}", self.packet_id),
            ContextSourceKind::Task,
            ContextRole::Evidence,
            self.context_summary(),
        );
        item.authority = ContextAuthority::Derived;
        item.visibility = ContextVisibility::Shared;
        item.score = self.confidence;
        item.evidence = self
            .source_refs
            .iter()
            .map(|source| source.reference.clone())
            .collect();
        item
    }

    pub fn add_tool_invocation_source(&mut self, invocation: &ToolInvocationRecord) {
        let reference = invocation.evidence_reference();
        self.source_refs.push(MatrixEvidenceSourceRef {
            kind: "tool_invocation".to_string(),
            reference: reference.clone(),
            summary: invocation.evidence_summary(),
        });
        if invocation.is_error.unwrap_or(false) {
            self.anomaly_evidence.push(serde_json::json!({
                "kind": "tool_invocation_failure",
                "tool_name": invocation.tool_name,
                "tool_call_id": invocation.tool_call_id,
                "status": invocation.status.as_str(),
                "failure_kind": invocation.failure_kind.map(|kind| kind.as_str()),
                "source_ref": reference,
            }));
        }
        self.refresh_readiness();
    }

    pub fn add_agent_evidence_source(&mut self, evidence: &AgentEvidence) {
        self.source_refs.push(MatrixEvidenceSourceRef {
            kind: "agent_evidence".to_string(),
            reference: evidence.reference.clone(),
            summary: evidence.summary.clone(),
        });
        self.attribution_candidates.push(serde_json::json!({
            "kind": "agent_evidence",
            "node_id": evidence.node_id,
            "evidence_id": evidence.id,
            "evidence_kind": evidence.kind,
            "source_ref": evidence.reference,
            "summary": evidence.summary,
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

    fn context_summary(&self) -> String {
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
    use crate::agent_protocol::AgentEvidence;
    use crate::tool_invocation::{ToolFailureKind, ToolInvocationRecord};
    use crate::tool_orchestrator::ToolSafetyCategory;

    #[test]
    fn evidence_packet_accepts_tool_invocation_source_without_output_copy() {
        let output = (0..80)
            .map(|idx| {
                format!(
                    "line {idx} unique-matrix-output-token-{idx} {}",
                    "x".repeat(24)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let invocation = ToolInvocationRecord::started(
            "session-1",
            1,
            "toolu-matrix",
            "bash",
            "collect",
            ToolSafetyCategory::WriteLocal,
            100,
        )
        .failed_with_output_policy(ToolFailureKind::ExecutionError, &output, 160, 3);
        let mut packet = MatrixEvidencePacket::new("supplier shortage risk");

        packet.add_tool_invocation_source(&invocation);

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
        let evidence = AgentEvidence {
            id: "evidence-1".to_string(),
            node_id: "planner".to_string(),
            kind: "tool_invocation".to_string(),
            reference: "tool-output:toolu-1:abc".to_string(),
            summary: "tool `bash`, status completed".to_string(),
            created_at_ms: 123,
        };
        let mut packet = MatrixEvidencePacket::new("bom explosion changed");

        packet.add_agent_evidence_source(&evidence);

        assert_eq!(packet.source_refs.len(), 1);
        assert_eq!(packet.source_refs[0].kind, "agent_evidence");
        assert_eq!(packet.attribution_candidates.len(), 1);
        assert!(packet.missing_evidence.is_empty());
        assert!(packet.confidence > 0.30);
    }
}
