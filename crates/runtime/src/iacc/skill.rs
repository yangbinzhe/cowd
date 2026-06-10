use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{IaccEvidencePacket, IaccIncident, IaccOperationalAnalysis};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccSkillExecutionContext {
    pub incident_id: String,
    pub skill_id: String,
    #[serde(default)]
    pub attention_id: Option<String>,
    #[serde(default)]
    pub evidence_packet_id: Option<String>,
    #[serde(default)]
    pub analysis_id: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub metric_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccSkillToolCall {
    pub tool_name: String,
    pub purpose: String,
    #[serde(default)]
    pub input_refs: Vec<String>,
    pub expected_output: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccSkillTelemetry {
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub elapsed_ms: u64,
    pub tool_call_count: usize,
    pub evidence_ref_count: usize,
    pub confidence: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccSkillManifest {
    pub skill_id: String,
    pub role: String,
    pub domain: String,
    #[serde(default)]
    pub input_fact_types: Vec<String>,
    #[serde(default)]
    pub input_metric_keys: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    pub analysis_method: String,
    #[serde(default)]
    pub output_actions: Vec<String>,
    pub quality_gate: String,
    pub success_criteria: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccSkillPlan {
    pub incident_id: String,
    #[serde(default)]
    pub selected_skills: Vec<IaccSkillManifest>,
    #[serde(default)]
    pub evidence_requirements: Vec<String>,
    #[serde(default)]
    pub planned_agent_nodes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccSkillRun {
    #[serde(default)]
    pub execution_id: Option<String>,
    pub incident_id: String,
    pub skill_id: String,
    pub status: String,
    pub summary: String,
    #[serde(default)]
    pub recommended_actions: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    #[serde(default)]
    pub agent_node_id: Option<String>,
    #[serde(default)]
    pub execution_context: Option<IaccSkillExecutionContext>,
    #[serde(default)]
    pub tool_plan: Vec<IaccSkillToolCall>,
    #[serde(default)]
    pub telemetry: Option<IaccSkillTelemetry>,
    #[serde(default)]
    pub structured_report: Value,
}

#[must_use]
pub fn server_manufacturing_skill_pack() -> Vec<IaccSkillManifest> {
    vec![
        skill(
            "supply-risk-analyst",
            "Supply Risk Analyst",
            &["supply.material_shortage", "supplier.commit_change"],
            &["material_shortage_risk", "supplier_commit_variance"],
            &["metric_lineage", "supplier_commit", "inventory_position"],
            &["supplier_recovery", "material_allocation_review"],
            "Trace shortage facts through supplier, inventory, and order delivery impact.",
            "shortage impact is bounded and recovery owner is identified",
        ),
        skill(
            "material-clear-to-build-analyst",
            "Material Clear-to-Build Analyst",
            &[
                "bom.change",
                "supply.material_shortage",
                "inventory.position",
            ],
            &["material_shortage_risk", "inventory_coverage_weeks"],
            &["bom_snapshot", "substitution_rule", "inventory_lot"],
            &["clear_to_build_check", "substitution_review"],
            "Compare BOM demand, available supply, substitutions, and weekly build plan.",
            "CTB blocker list is deterministic and tied to component refs",
        ),
        skill(
            "capacity-risk-analyst",
            "Capacity Risk Analyst",
            &["manufacturing.capacity_load", "plan.weekly_commit"],
            &["work_center_load", "order_delivery_risk"],
            &["work_center_calendar", "routing", "weekly_plan"],
            &["capacity_rebalance", "schedule_review"],
            "Project work-center load and identify bottleneck propagation to shipment risk.",
            "capacity bottleneck has owner, period, and mitigation path",
        ),
        skill(
            "quality-trace-analyst",
            "Quality Trace Analyst",
            &["quality.issue", "quality.escape"],
            &["first_pass_yield", "quality_escape_risk"],
            &["quality_lot", "test_station", "affected_orders"],
            &["containment_review", "root_cause_trace"],
            "Trace quality issue from lot/test evidence to affected product and customer scope.",
            "affected scope and containment action are explicit",
        ),
        skill(
            "delivery-risk-analyst",
            "Delivery Risk Analyst",
            &[
                "customer.order_change",
                "shipment.delay",
                "supply.material_shortage",
            ],
            &["order_delivery_risk", "revenue_at_risk"],
            &[
                "customer_order",
                "shipment_plan",
                "material_and_capacity_risk",
            ],
            &["delivery_commit_review", "customer_escalation_plan"],
            "Combine material, capacity, quality, and shipment signals into delivery risk.",
            "delivery commitment risk is quantified with next action",
        ),
        skill(
            "procurement-coordinator",
            "Procurement Coordinator",
            &[
                "purchase_order.delay",
                "supplier.commit_change",
                "supply.material_shortage",
            ],
            &["supplier_commit_variance", "material_shortage_risk"],
            &["purchase_order", "supplier_contact", "allocation_decision"],
            &["supplier_followup", "expedite_request"],
            "Prepare governed supplier follow-up tasks with evidence and expected recovery date.",
            "supplier task is ready for cross-plane dispatch",
        ),
        skill(
            "plan-change-impact-analyst",
            "Plan Change Impact Analyst",
            &["plan.weekly_commit", "bom.change", "customer.order_change"],
            &[
                "order_delivery_risk",
                "work_center_load",
                "inventory_coverage_weeks",
            ],
            &["weekly_plan", "bom_diff", "demand_change"],
            &["plan_impact_review", "scenario_compare"],
            "Propagate plan and BOM changes across material, capacity, and delivery metrics.",
            "impact path is explainable and scoped by week/entity",
        ),
    ]
}

#[must_use]
pub fn plan_server_manufacturing_skills(
    incident: &IaccIncident,
    analysis: Option<&IaccOperationalAnalysis>,
    packet: Option<&IaccEvidencePacket>,
    limit: usize,
) -> IaccSkillPlan {
    let metric_keys = metric_keys(analysis, packet);
    let text = format!(
        "{} {}",
        incident.title.to_lowercase(),
        metric_keys.join(" ").to_lowercase()
    );
    let mut skills = server_manufacturing_skill_pack();
    skills.sort_by(|left, right| {
        score_skill(right, &text, &metric_keys).cmp(&score_skill(left, &text, &metric_keys))
    });
    let selected_skills = skills
        .into_iter()
        .filter(|skill| score_skill(skill, &text, &metric_keys) > 0)
        .take(limit)
        .collect::<Vec<_>>();
    let selected_skills = if selected_skills.is_empty() {
        server_manufacturing_skill_pack()
            .into_iter()
            .take(2)
            .collect()
    } else {
        selected_skills
    };
    let evidence_requirements = selected_skills
        .iter()
        .flat_map(|skill| skill.required_evidence.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let planned_agent_nodes = selected_skills
        .iter()
        .map(|skill| skill_agent_node_id(&skill.skill_id))
        .collect::<Vec<_>>();
    IaccSkillPlan {
        incident_id: incident.incident_id.clone(),
        selected_skills,
        evidence_requirements,
        planned_agent_nodes,
    }
}

#[must_use]
pub fn run_server_manufacturing_skill(
    incident: &IaccIncident,
    skill: &IaccSkillManifest,
    analysis: Option<&IaccOperationalAnalysis>,
    packet: Option<&IaccEvidencePacket>,
) -> IaccSkillRun {
    let context = IaccSkillExecutionContext {
        incident_id: incident.incident_id.clone(),
        skill_id: skill.skill_id.clone(),
        attention_id: incident.attention_id.clone(),
        evidence_packet_id: incident.evidence_packet_id.clone(),
        analysis_id: analysis.map(|item| item.analysis_id.clone()),
        evidence_refs: packet
            .map(|item| {
                item.source_refs
                    .iter()
                    .map(|source| source.reference.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        metric_keys: skill.input_metric_keys.clone(),
    };
    let started_at = Utc::now();
    let tool_plan = skill
        .tools
        .iter()
        .enumerate()
        .map(|(index, tool_name)| IaccSkillToolCall {
            tool_name: tool_name.clone(),
            purpose: format!("step_{}_{}", index + 1, skill.analysis_method),
            input_refs: context.evidence_refs.clone(),
            expected_output: skill.success_criteria.clone(),
        })
        .collect::<Vec<_>>();
    let structured_report = serde_json::json!({
        "incident_id": incident.incident_id,
        "skill_id": skill.skill_id,
        "analysis_method": skill.analysis_method,
        "success_criteria": skill.success_criteria,
        "evidence_refs": context.evidence_refs,
        "metric_keys": context.metric_keys,
        "recommended_actions": skill.output_actions,
        "quality_gate": skill.quality_gate,
        "quality_gate_status": if packet
            .map(|item| item.confidence >= 0.75 && item.missing_evidence.is_empty())
            .unwrap_or(false)
        {
            "pass"
        } else {
            "review"
        },
        "evidence_packet_confidence": packet.map(|item| item.confidence),
    });
    let completed_at = Utc::now();
    IaccSkillRun {
        execution_id: Some(format!("skill-execution-{}", uuid::Uuid::new_v4())),
        incident_id: incident.incident_id.clone(),
        skill_id: skill.skill_id.clone(),
        status: "completed".to_string(),
        summary: format!(
            "{} prepared governed analysis for {} with {} tool calls",
            skill.role,
            incident.title,
            tool_plan.len()
        ),
        recommended_actions: skill.output_actions.clone(),
        required_evidence: skill.required_evidence.clone(),
        agent_node_id: Some(skill_agent_node_id(&skill.skill_id)),
        execution_context: Some(context),
        tool_plan,
        telemetry: Some(IaccSkillTelemetry {
            started_at,
            completed_at,
            elapsed_ms: completed_at
                .signed_duration_since(started_at)
                .num_milliseconds()
                .max(0) as u64,
            tool_call_count: skill.tools.len(),
            evidence_ref_count: packet
                .map(|item| item.source_refs.len())
                .unwrap_or_default(),
            confidence: packet.map(|item| item.confidence).unwrap_or(0.5),
        }),
        structured_report,
    }
}

#[must_use]
pub fn skill_agent_node_id(skill_id: &str) -> String {
    format!("iacc_skill_{}", skill_id.replace('-', "_"))
}

fn skill(
    skill_id: &str,
    role: &str,
    input_fact_types: &[&str],
    input_metric_keys: &[&str],
    required_evidence: &[&str],
    output_actions: &[&str],
    analysis_method: &str,
    success_criteria: &str,
) -> IaccSkillManifest {
    IaccSkillManifest {
        skill_id: skill_id.to_string(),
        role: role.to_string(),
        domain: "server_manufacturing".to_string(),
        input_fact_types: input_fact_types
            .iter()
            .map(|value| value.to_string())
            .collect(),
        input_metric_keys: input_metric_keys
            .iter()
            .map(|value| value.to_string())
            .collect(),
        required_evidence: required_evidence
            .iter()
            .map(|value| value.to_string())
            .collect(),
        tools: vec![
            "iacc.metric_lineage".to_string(),
            "iacc.entity_impact_trace".to_string(),
            "iacc.evidence_packet".to_string(),
            "iacc.cross_plane_preflight".to_string(),
        ],
        analysis_method: analysis_method.to_string(),
        output_actions: output_actions
            .iter()
            .map(|value| value.to_string())
            .collect(),
        quality_gate: "evidence_quality_gate_required".to_string(),
        success_criteria: success_criteria.to_string(),
    }
}

fn metric_keys(
    analysis: Option<&IaccOperationalAnalysis>,
    packet: Option<&IaccEvidencePacket>,
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

fn score_skill(skill: &IaccSkillManifest, text: &str, metric_keys: &[String]) -> usize {
    let metric_score = skill
        .input_metric_keys
        .iter()
        .filter(|metric| metric_keys.contains(metric))
        .count()
        * 10;
    let text_score = skill
        .input_metric_keys
        .iter()
        .chain(skill.input_fact_types.iter())
        .filter(|term| {
            text.contains(term.as_str()) || text.contains(term.replace('_', " ").as_str())
        })
        .count();
    metric_score + text_score
}
