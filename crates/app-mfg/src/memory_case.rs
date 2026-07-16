use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use matrix_core::MatrixEvidencePacket;

use super::{MfgActionExecution, MfgIncident, MfgOperationalAnalysis};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgMemoryCase {
    pub case_id: String,
    pub incident_id: String,
    #[serde(default)]
    pub analysis_id: Option<String>,
    #[serde(default)]
    pub evidence_packet_id: Option<String>,
    pub title: String,
    pub problem_signature: String,
    #[serde(default)]
    pub entity_refs: Vec<String>,
    #[serde(default)]
    pub metric_keys: Vec<String>,
    #[serde(default)]
    pub root_causes: Vec<String>,
    #[serde(default)]
    pub actions_taken: Vec<String>,
    #[serde(default)]
    pub execution_receipts: Vec<String>,
    #[serde(default)]
    pub feedback_summary: Option<String>,
    pub outcome: String,
    #[serde(default)]
    pub playbook_id: Option<String>,
    pub memory_summary: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgPlaybookStep {
    pub step_id: String,
    pub title: String,
    pub owner_role: String,
    pub action_type: String,
    pub expected_effect: String,
    #[serde(default)]
    pub required_evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgPlaybook {
    pub playbook_id: String,
    #[serde(default = "default_playbook_revision")]
    pub revision: u64,
    pub domain: String,
    pub scenario: String,
    #[serde(default)]
    pub trigger_fact_types: Vec<String>,
    #[serde(default)]
    pub metric_keys: Vec<String>,
    #[serde(default)]
    pub recommended_steps: Vec<MfgPlaybookStep>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    pub quality_gate_policy: String,
    pub cross_plane_policy: String,
    #[serde(default)]
    pub success_metrics: Vec<String>,
    #[serde(default)]
    pub created_from_case_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn default_playbook_revision() -> u64 {
    1
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCasePromotion {
    pub memory_case: MfgMemoryCase,
    pub playbook: MfgPlaybook,
}

impl MfgMemoryCase {
    #[must_use]
    pub fn from_closed_loop(
        incident: &MfgIncident,
        analysis: Option<&MfgOperationalAnalysis>,
        packet: Option<&MatrixEvidencePacket>,
        executions: &[MfgActionExecution],
    ) -> Self {
        let now = Utc::now();
        let metric_keys = metric_keys(analysis, packet);
        let entity_refs = entity_refs(analysis, packet);
        let root_causes = analysis
            .map(|value| {
                value
                    .attribution_candidates
                    .iter()
                    .map(|candidate| candidate.summary.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let actions_taken = executions
            .iter()
            .map(|execution| {
                format!(
                    "{} [{}] -> {}",
                    execution.title, execution.mode, execution.status
                )
            })
            .collect::<Vec<_>>();
        let execution_receipts = executions
            .iter()
            .flat_map(|execution| {
                execution
                    .cross_plane_receipts
                    .iter()
                    .map(|receipt| receipt.cross_plane_receipt_id.clone())
                    .chain(std::iter::once(execution.execution_id.clone()))
            })
            .collect::<Vec<_>>();
        let feedback_summary = executions.iter().rev().find_map(|execution| {
            execution
                .feedback
                .as_ref()
                .map(|feedback| format!("{}: {}", feedback.outcome, feedback.note))
        });
        let outcome = if executions
            .iter()
            .any(|execution| execution.status == "feedback_resolved")
            || incident.status == "closed"
        {
            "resolved"
        } else {
            "captured"
        }
        .to_string();
        let problem_signature = problem_signature(&incident.title, &metric_keys, &entity_refs);
        let memory_summary = format!(
            "{} | outcome={} | metrics={} | entities={} | actions={}",
            incident.title,
            outcome,
            metric_keys.join(","),
            entity_refs.join(","),
            actions_taken.len()
        );
        Self {
            case_id: format!("case-{}", uuid::Uuid::new_v4()),
            incident_id: incident.incident_id.clone(),
            analysis_id: analysis.map(|value| value.analysis_id.clone()),
            evidence_packet_id: incident.evidence_packet_id.clone(),
            title: incident.title.clone(),
            problem_signature,
            entity_refs,
            metric_keys,
            root_causes,
            actions_taken,
            execution_receipts,
            feedback_summary,
            outcome,
            playbook_id: None,
            memory_summary,
            created_at: now,
        }
    }
}

impl MfgPlaybook {
    #[must_use]
    pub fn from_memory_case(
        case: &MfgMemoryCase,
        analysis: Option<&MfgOperationalAnalysis>,
    ) -> Self {
        let now = Utc::now();
        let recommended_steps = analysis
            .map(|value| {
                value
                    .recommended_actions
                    .iter()
                    .map(|action| MfgPlaybookStep {
                        step_id: format!("step-{}", uuid::Uuid::new_v4()),
                        title: action.title.clone(),
                        owner_role: action.owner_role.clone(),
                        action_type: action.action_type.clone(),
                        expected_effect: action.expected_effect.clone(),
                        required_evidence: action.required_evidence.clone(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                vec![MfgPlaybookStep {
                    step_id: format!("step-{}", uuid::Uuid::new_v4()),
                    title: "Review evidence and assign accountable owner".to_string(),
                    owner_role: "operations_analyst".to_string(),
                    action_type: "human_review".to_string(),
                    expected_effect: "Confirm operational owner before dispatch".to_string(),
                    required_evidence: vec!["evidence_packet".to_string()],
                }]
            });
        let required_evidence = recommended_steps
            .iter()
            .flat_map(|step| step.required_evidence.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        Self {
            playbook_id: format!("playbook-{}", uuid::Uuid::new_v4()),
            revision: 1,
            domain: domain_for_case(case),
            scenario: scenario_for_case(case),
            trigger_fact_types: Vec::new(),
            metric_keys: case.metric_keys.clone(),
            recommended_steps,
            required_evidence,
            quality_gate_policy: "require_evidence_quality_gate_before_commit".to_string(),
            cross_plane_policy: "dry_run_first_then_identity_grant_commit".to_string(),
            success_metrics: case.metric_keys.clone(),
            created_from_case_id: Some(case.case_id.clone()),
            created_at: now,
            updated_at: now,
        }
    }
}

fn metric_keys(
    analysis: Option<&MfgOperationalAnalysis>,
    packet: Option<&MatrixEvidencePacket>,
) -> Vec<String> {
    let mut values = analysis
        .map(|value| {
            value
                .attribution_candidates
                .iter()
                .filter_map(|candidate| candidate.metric_id.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if values.is_empty() {
        if let Some(packet) = packet {
            for evidence in &packet.metric_evidence {
                if let Some(metric_id) = evidence
                    .get("metric_id")
                    .and_then(serde_json::Value::as_str)
                {
                    values.push(metric_id.to_string());
                }
            }
        }
    }
    values.sort();
    values.dedup();
    values
}

fn entity_refs(
    analysis: Option<&MfgOperationalAnalysis>,
    packet: Option<&MatrixEvidencePacket>,
) -> Vec<String> {
    let mut values = analysis
        .map(|value| {
            value
                .attribution_candidates
                .iter()
                .filter_map(|candidate| candidate.entity_ref.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if values.is_empty() {
        if let Some(packet) = packet {
            if let Some(entity_ref) = packet
                .business_context
                .get("entity_ref")
                .and_then(serde_json::Value::as_str)
            {
                values.push(entity_ref.to_string());
            }
        }
    }
    values.sort();
    values.dedup();
    values
}

fn problem_signature(title: &str, metric_keys: &[String], entity_refs: &[String]) -> String {
    format!(
        "{}|{}|{}",
        title.to_lowercase(),
        metric_keys.join(","),
        entity_refs.join(",")
    )
}

fn domain_for_case(case: &MfgMemoryCase) -> String {
    if case
        .metric_keys
        .iter()
        .any(|metric| metric.contains("shortage") || metric.contains("supply"))
    {
        "server_manufacturing_supply".to_string()
    } else if case
        .metric_keys
        .iter()
        .any(|metric| metric.contains("quality"))
    {
        "server_manufacturing_quality".to_string()
    } else {
        "server_manufacturing_operations".to_string()
    }
}

fn scenario_for_case(case: &MfgMemoryCase) -> String {
    if case
        .metric_keys
        .iter()
        .any(|metric| metric.contains("shortage"))
    {
        "material_shortage_recovery".to_string()
    } else if case
        .metric_keys
        .iter()
        .any(|metric| metric.contains("delivery"))
    {
        "delivery_risk_recovery".to_string()
    } else {
        "operational_exception_recovery".to_string()
    }
}
