use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use matrix_core::MatrixEvidencePacket;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAttributionCandidate {
    pub cause_id: String,
    pub cause_type: String,
    pub summary: String,
    #[serde(default)]
    pub metric_id: Option<String>,
    #[serde(default)]
    pub entity_ref: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub confidence: f32,
    pub priority_score: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgImpactPath {
    pub path_id: String,
    pub from_entity: String,
    pub to_scope: String,
    pub impact_type: String,
    pub severity: String,
    pub summary: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub confidence: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgRecommendedAction {
    pub action_id: String,
    pub action_type: String,
    pub title: String,
    pub owner_role: String,
    pub priority: String,
    pub expected_effect: String,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    #[serde(default)]
    pub command_hint: Option<String>,
    pub governance: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgOperationalAnalysis {
    pub analysis_id: String,
    pub incident_id: String,
    pub evidence_packet_id: String,
    #[serde(default)]
    pub attribution_candidates: Vec<MfgAttributionCandidate>,
    #[serde(default)]
    pub impact_paths: Vec<MfgImpactPath>,
    #[serde(default)]
    pub recommended_actions: Vec<MfgRecommendedAction>,
    pub confidence: f32,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

impl MfgOperationalAnalysis {
    #[must_use]
    pub fn from_evidence(incident_id: impl Into<String>, packet: &MatrixEvidencePacket) -> Self {
        let mut analysis = Self {
            analysis_id: format!("analysis-{}", uuid::Uuid::new_v4()),
            incident_id: incident_id.into(),
            evidence_packet_id: packet.packet_id.clone(),
            attribution_candidates: Vec::new(),
            impact_paths: Vec::new(),
            recommended_actions: Vec::new(),
            confidence: packet.confidence,
            status: "draft".to_string(),
            created_at: Utc::now(),
        };

        for change in &packet.change_evidence {
            let metric_id = json_string(change, "metric_id");
            let entity_ref =
                json_string(change, "entity_ref").unwrap_or_else(|| "enterprise".to_string());
            let delta = json_f64(change, "delta").unwrap_or(0.0);
            let severity =
                json_string(change, "severity_hint").unwrap_or_else(|| "unknown".to_string());
            let evidence_refs = json_string_array(change, "source_fact_refs");
            let cause_type = classify_cause(metric_id.as_deref(), &entity_ref, delta);
            analysis
                .attribution_candidates
                .push(MfgAttributionCandidate {
                    cause_id: format!("cause-{}", uuid::Uuid::new_v4()),
                    cause_type: cause_type.to_string(),
                    summary: attribution_summary(metric_id.as_deref(), &entity_ref, delta),
                    metric_id: metric_id.clone(),
                    entity_ref: Some(entity_ref.clone()),
                    evidence_refs: evidence_refs.clone(),
                    confidence: packet.confidence.max(0.55),
                    priority_score: priority_for(&severity, delta, packet.confidence),
                });
            analysis.impact_paths.push(MfgImpactPath {
                path_id: format!("impact-{}", uuid::Uuid::new_v4()),
                from_entity: entity_ref.clone(),
                to_scope: impact_scope(metric_id.as_deref()).to_string(),
                impact_type: impact_type(metric_id.as_deref()).to_string(),
                severity: severity.clone(),
                summary: impact_summary(metric_id.as_deref(), &entity_ref, delta),
                evidence_refs: evidence_refs.clone(),
                confidence: packet.confidence.max(0.55),
            });
            analysis.recommended_actions.push(recommended_action(
                metric_id.as_deref(),
                &severity,
                &evidence_refs,
            ));
        }

        if analysis.attribution_candidates.is_empty() {
            let evidence_refs = packet
                .source_refs
                .iter()
                .map(|source| source.reference.clone())
                .collect::<Vec<_>>();
            analysis
                .attribution_candidates
                .push(MfgAttributionCandidate {
                cause_id: format!("cause-{}", uuid::Uuid::new_v4()),
                cause_type: "evidence_gap".to_string(),
                summary:
                    "Insufficient typed change evidence; collect source-system facts before action"
                        .to_string(),
                metric_id: None,
                entity_ref: None,
                evidence_refs: evidence_refs.clone(),
                confidence: 0.35,
                priority_score: 0.4,
            });
            analysis.impact_paths.push(MfgImpactPath {
                path_id: format!("impact-{}", uuid::Uuid::new_v4()),
                from_entity: "unknown".to_string(),
                to_scope: "operations".to_string(),
                impact_type: "analysis_gap".to_string(),
                severity: "unknown".to_string(),
                summary:
                    "No deterministic impact path can be produced without metric/change evidence"
                        .to_string(),
                evidence_refs,
                confidence: 0.35,
            });
            analysis
                .recommended_actions
                .push(recommended_action(None, "unknown", &[]));
        }

        analysis.confidence = analysis
            .attribution_candidates
            .iter()
            .map(|candidate| candidate.confidence)
            .fold(packet.confidence, f32::max)
            .min(0.92);
        analysis.status = if analysis.confidence >= 0.65 {
            "ready_for_review".to_string()
        } else {
            "needs_more_evidence".to_string()
        };
        analysis
    }
}

fn json_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn json_f64(value: &serde_json::Value, key: &str) -> Option<f64> {
    value.get(key).and_then(serde_json::Value::as_f64)
}

fn json_string_array(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn classify_cause(metric_id: Option<&str>, entity_ref: &str, delta: f64) -> &'static str {
    let metric = metric_id.unwrap_or_default();
    if metric.contains("shortage") {
        "supply_constraint"
    } else if metric.contains("quality") {
        "quality_escape"
    } else if metric.contains("capacity") || metric.contains("work_center") {
        "capacity_constraint"
    } else if entity_ref.starts_with("component:") {
        "supply_constraint"
    } else if metric.contains("bom") || metric.contains("demand") || delta > 0.0 {
        "planning_demand_change"
    } else {
        "metric_delta"
    }
}

fn priority_for(severity: &str, delta: f64, confidence: f32) -> f32 {
    let severity_score = match severity {
        "critical" => 1.0,
        "warning" => 0.65,
        "normal" => 0.25,
        _ => 0.4,
    };
    let delta_score = (delta.abs() / 100.0).min(1.0) as f32;
    (severity_score * 0.45 + delta_score * 0.35 + confidence * 0.20).min(1.0)
}

fn attribution_summary(metric_id: Option<&str>, entity_ref: &str, delta: f64) -> String {
    format!(
        "Metric {} changed by {} on {}; treat this as the primary deterministic cause candidate",
        metric_id.unwrap_or("unknown"),
        delta,
        entity_ref
    )
}

fn impact_scope(metric_id: Option<&str>) -> &'static str {
    let metric = metric_id.unwrap_or_default();
    if metric.contains("shortage") {
        "supply_to_production"
    } else if metric.contains("capacity") || metric.contains("work_center") {
        "capacity_to_output"
    } else if metric.contains("bom") || metric.contains("demand") {
        "plan_to_material_and_capacity"
    } else if metric.contains("quality") {
        "quality_to_delivery"
    } else {
        "enterprise_operations"
    }
}

fn impact_type(metric_id: Option<&str>) -> &'static str {
    let metric = metric_id.unwrap_or_default();
    if metric.contains("shortage") {
        "material_availability_risk"
    } else if metric.contains("capacity") || metric.contains("work_center") {
        "capacity_throughput_risk"
    } else if metric.contains("bom") || metric.contains("demand") {
        "schedule_and_inventory_risk"
    } else if metric.contains("quality") {
        "delivery_quality_risk"
    } else {
        "operational_metric_risk"
    }
}

fn impact_summary(metric_id: Option<&str>, entity_ref: &str, delta: f64) -> String {
    format!(
        "{} on {} can propagate through {} with delta {}",
        metric_id.unwrap_or("unknown metric"),
        entity_ref,
        impact_scope(metric_id),
        delta
    )
}

fn recommended_action(
    metric_id: Option<&str>,
    severity: &str,
    evidence_refs: &[String],
) -> MfgRecommendedAction {
    let metric = metric_id.unwrap_or_default();
    let priority = if severity == "critical" { "p0" } else { "p1" };
    if metric.contains("shortage") {
        return MfgRecommendedAction {
            action_id: format!("action-{}", uuid::Uuid::new_v4()),
            action_type: "supplier_recovery".to_string(),
            title: "Start supplier recovery and material allocation review".to_string(),
            owner_role: "supply_planner".to_string(),
            priority: priority.to_string(),
            expected_effect: "Reduce material availability risk before production commitment"
                .to_string(),
            required_evidence: evidence_refs.to_vec(),
            command_hint: Some("mfg://actions/supply/recovery-review".to_string()),
            governance: "human_review_required".to_string(),
        };
    }
    if metric.contains("bom") || metric.contains("demand") {
        return MfgRecommendedAction {
            action_id: format!("action-{}", uuid::Uuid::new_v4()),
            action_type: "plan_bom_reconciliation".to_string(),
            title: "Reconcile weekly demand, BOM explosion, material and capacity commitments"
                .to_string(),
            owner_role: "operations_planner".to_string(),
            priority: priority.to_string(),
            expected_effect: "Align plan changes with supply and production constraints"
                .to_string(),
            required_evidence: evidence_refs.to_vec(),
            command_hint: Some("mfg://actions/plan/reconcile-bom-demand".to_string()),
            governance: "human_review_required".to_string(),
        };
    }
    if metric.contains("capacity") || metric.contains("work_center") {
        return MfgRecommendedAction {
            action_id: format!("action-{}", uuid::Uuid::new_v4()),
            action_type: "capacity_rebalance".to_string(),
            title: "Rebalance weekly capacity and protect committed output".to_string(),
            owner_role: "production_planner".to_string(),
            priority: priority.to_string(),
            expected_effect: "Reduce work center overload before it constrains shipment readiness"
                .to_string(),
            required_evidence: evidence_refs.to_vec(),
            command_hint: Some("mfg://actions/capacity/rebalance-week".to_string()),
            governance: "human_review_required".to_string(),
        };
    }
    if metric.contains("quality") {
        return MfgRecommendedAction {
            action_id: format!("action-{}", uuid::Uuid::new_v4()),
            action_type: "quality_containment".to_string(),
            title: "Start quality containment and affected order assessment".to_string(),
            owner_role: "quality_engineer".to_string(),
            priority: priority.to_string(),
            expected_effect: "Contain quality escape risk and identify affected shipment scope"
                .to_string(),
            required_evidence: evidence_refs.to_vec(),
            command_hint: Some("mfg://actions/quality/containment-review".to_string()),
            governance: "human_review_required".to_string(),
        };
    }
    MfgRecommendedAction {
        action_id: format!("action-{}", uuid::Uuid::new_v4()),
        action_type: "evidence_review".to_string(),
        title: "Review evidence and assign domain owner".to_string(),
        owner_role: "operations_analyst".to_string(),
        priority: priority.to_string(),
        expected_effect: "Convert detected metric risk into a governed operating response"
            .to_string(),
        required_evidence: evidence_refs.to_vec(),
        command_hint: Some("mfg://actions/operations/evidence-review".to_string()),
        governance: "human_review_required".to_string(),
    }
}
