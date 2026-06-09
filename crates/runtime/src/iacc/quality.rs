use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::IaccEvidencePacket;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccQualityGateDecision {
    pub gate_id: String,
    pub target_ref: String,
    pub gate_type: String,
    pub decision: String,
    pub score: f32,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub required_actions: Vec<String>,
    pub created_at: DateTime<Utc>,
}

impl IaccQualityGateDecision {
    #[must_use]
    pub fn for_evidence_packet(packet: &IaccEvidencePacket) -> Self {
        let mut score = 0.0f32;
        let mut reasons = Vec::new();
        let mut required_actions = Vec::new();

        if packet.source_refs.is_empty() {
            reasons.push("missing_source_refs".to_string());
            required_actions.push("attach_source_refs".to_string());
        } else {
            score += 0.15;
            reasons.push("source_refs_present".to_string());
        }

        if packet.metric_evidence.is_empty() {
            reasons.push("missing_metric_evidence".to_string());
            required_actions.push("collect_metric_evidence".to_string());
        } else {
            score += 0.2;
            reasons.push("metric_evidence_present".to_string());
        }

        if packet.change_evidence.is_empty() {
            reasons.push("missing_change_evidence".to_string());
            required_actions.push("detect_change_events".to_string());
        } else {
            score += 0.2;
            reasons.push("change_evidence_present".to_string());
        }

        if packet.attribution_candidates.is_empty() {
            reasons.push("missing_attribution_candidates".to_string());
            required_actions.push("run_incident_analysis".to_string());
        } else {
            score += 0.18;
            reasons.push("attribution_candidates_present".to_string());
        }

        if packet.impact_paths.is_empty() {
            reasons.push("missing_impact_paths".to_string());
            required_actions.push("run_impact_analysis".to_string());
        } else {
            score += 0.18;
            reasons.push("impact_paths_present".to_string());
        }

        score += (packet.confidence * 0.18).min(0.18);
        if packet.confidence >= 0.65 {
            reasons.push("packet_confidence_acceptable".to_string());
        } else {
            reasons.push("packet_confidence_low".to_string());
            required_actions.push("raise_evidence_confidence".to_string());
        }

        if packet.attribution_candidates.is_empty() || packet.impact_paths.is_empty() {
            score = score.min(0.68);
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
            gate_id: format!("quality-gate-{}", uuid::Uuid::new_v4()),
            target_ref: format!("iacc:evidence:{}", packet.packet_id),
            gate_type: "evidence_insight_quality".to_string(),
            decision: decision.to_string(),
            score,
            reasons,
            required_actions,
            created_at: Utc::now(),
        }
    }
}
