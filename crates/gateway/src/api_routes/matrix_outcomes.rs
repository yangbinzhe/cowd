use cowd_app_protocol::{
    ApplicationExecutionSummaryCounterV1, ApplicationExecutionSummaryKindV1,
    ApplicationExecutionSummaryRefV1, ApplicationExecutionSummaryStatusV1,
    ApplicationExecutionSummaryV1,
};
use matrix_core::{MatrixDataPlaneIngestPlan, MatrixEvidencePacket, MatrixFact};
use serde_json::Value;

use super::AppState;

pub(super) async fn append_matrix_execution_summary(
    state: &AppState,
    session_id: Option<&str>,
    summary: ApplicationExecutionSummaryV1,
) -> Result<(), String> {
    let Some(session_id) = session_id.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };
    state
        .services
        .session
        .append_application_execution_summary(session_id, &summary)
        .await
        .map(|_| ())
}

pub(super) fn matrix_ingest_plan_summary(
    plan: &MatrixDataPlaneIngestPlan,
) -> ApplicationExecutionSummaryV1 {
    ApplicationExecutionSummaryV1 {
        schema_version: 1,
        summary_id: format!("structured-ingest:{}", plan.batch_id),
        kind: ApplicationExecutionSummaryKindV1::StructuredIngest,
        status: ApplicationExecutionSummaryStatusV1::Planned,
        title: format!("Structured ingest plan for {}", plan.fact_type),
        summary: format!(
            "Plan {} ingests {} estimated rows from {} partition {}.",
            plan.batch_id, plan.estimated_rows, plan.source_ref, plan.partition_ref
        ),
        domain: Some("matrix".to_string()),
        refs: vec![
            execution_ref(
                "structured_source",
                &plan.source_ref,
                Some(&plan.source_ref),
            ),
            execution_ref("structured_batch", &plan.batch_id, Some(&plan.fact_type)),
        ],
        evidence_refs: Vec::new(),
        metric_refs: plan.affected_metric_ids.clone(),
        counters: vec![counter(
            "estimated_rows",
            i64::try_from(plan.estimated_rows).unwrap_or(i64::MAX),
        )],
        occurred_at_ms: timestamp_ms(plan.planned_at.timestamp_millis()),
    }
}

pub(super) fn matrix_fact_summary(fact: &MatrixFact) -> ApplicationExecutionSummaryV1 {
    let mut refs = vec![execution_ref(
        "structured_fact",
        &fact.fact_id,
        Some(&fact.fact_type),
    )];
    if let Some(source_ref) = fact.source_ref.as_ref() {
        refs.push(execution_ref(
            "structured_source",
            source_ref,
            Some(source_ref),
        ));
    }
    ApplicationExecutionSummaryV1 {
        schema_version: 1,
        summary_id: format!("structured-fact:{}", fact.fact_id),
        kind: ApplicationExecutionSummaryKindV1::StructuredFact,
        status: ApplicationExecutionSummaryStatusV1::Succeeded,
        title: format!("Structured fact {}", fact.fact_type),
        summary: format!(
            "Fact {} of type {} references {} entities with confidence {:.2}.",
            fact.fact_id,
            fact.fact_type,
            fact.entity_refs.len(),
            fact.confidence
        ),
        domain: Some("matrix".to_string()),
        refs,
        evidence_refs: fact
            .source_ref
            .iter()
            .map(|source_ref| format!("structured-source:{source_ref}"))
            .collect(),
        metric_refs: fact.metric_key.iter().cloned().collect(),
        counters: vec![counter(
            "entity_refs",
            i64::try_from(fact.entity_refs.len()).unwrap_or(i64::MAX),
        )],
        occurred_at_ms: timestamp_ms(fact.event_time.timestamp_millis()),
    }
}

pub(super) fn matrix_evidence_packet_summary(
    packet: &MatrixEvidencePacket,
) -> ApplicationExecutionSummaryV1 {
    ApplicationExecutionSummaryV1 {
        schema_version: 1,
        summary_id: format!("structured-evidence:{}", packet.packet_id),
        kind: ApplicationExecutionSummaryKindV1::StructuredEvidence,
        status: if packet.missing_evidence.is_empty() {
            ApplicationExecutionSummaryStatusV1::Succeeded
        } else {
            ApplicationExecutionSummaryStatusV1::Partial
        },
        title: format!("Evidence packet {}", packet.packet_id),
        summary: format!(
            "Evidence packet for '{}' has {} metric items, {} change items and confidence {:.2}.",
            packet.problem_statement,
            packet.metric_evidence.len(),
            packet.change_evidence.len(),
            packet.confidence
        ),
        domain: Some("matrix".to_string()),
        refs: vec![execution_ref(
            "structured_evidence",
            &packet.packet_id,
            packet.attention_id.as_deref(),
        )],
        evidence_refs: packet
            .source_refs
            .iter()
            .map(|source| source.reference.clone())
            .collect(),
        metric_refs: packet
            .metric_evidence
            .iter()
            .filter_map(|item| item.get("metric_id").and_then(Value::as_str))
            .map(ToString::to_string)
            .collect(),
        counters: vec![
            counter(
                "metric_items",
                i64::try_from(packet.metric_evidence.len()).unwrap_or(i64::MAX),
            ),
            counter(
                "change_items",
                i64::try_from(packet.change_evidence.len()).unwrap_or(i64::MAX),
            ),
            counter(
                "missing_evidence",
                i64::try_from(packet.missing_evidence.len()).unwrap_or(i64::MAX),
            ),
        ],
        occurred_at_ms: timestamp_ms(packet.created_at.timestamp_millis()),
    }
}

fn execution_ref(
    ref_type: impl Into<String>,
    id: impl Into<String>,
    label: Option<&str>,
) -> ApplicationExecutionSummaryRefV1 {
    ApplicationExecutionSummaryRefV1 {
        ref_type: ref_type.into(),
        id: id.into(),
        label: label.map(ToString::to_string),
    }
}

fn counter(name: impl Into<String>, value: i64) -> ApplicationExecutionSummaryCounterV1 {
    ApplicationExecutionSummaryCounterV1 {
        name: name.into(),
        value,
    }
}

fn timestamp_ms(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}
