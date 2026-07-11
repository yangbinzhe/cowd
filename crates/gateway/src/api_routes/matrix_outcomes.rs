use matrix_core::{MatrixDataPlaneIngestPlan, MatrixEvidencePacket, MatrixFact};
use memory::store::session::SessionRecord;
use runtime::execution_outcome::{
    CowdExecutionOutcome, CowdExecutionOutcomeKind, CowdExecutionOutcomeStatus, CowdExecutionRef,
};
use serde_json::Value;

use super::AppState;

pub(super) async fn append_matrix_execution_outcome(
    state: &AppState,
    session_id: Option<&str>,
    outcome: CowdExecutionOutcome,
) -> Result<(), String> {
    let Some(session_id) = session_id.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };
    let Some(store) = state.services.session.unified_store() else {
        return Ok(());
    };
    ensure_matrix_outcome_session_record(state, session_id)
        .await
        .map_err(|error| format!("failed to prepare Matrix outcome session: {error}"))?;
    let event = outcome.to_runtime_event(session_id.to_string(), 0);
    store
        .append_runtime_event_allocating_sequence(&event)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub(super) fn matrix_ingest_plan_outcome(plan: &MatrixDataPlaneIngestPlan) -> CowdExecutionOutcome {
    CowdExecutionOutcome {
        outcome_id: format!("structured-ingest:{}", plan.batch_id),
        kind: CowdExecutionOutcomeKind::StructuredIngest,
        status: CowdExecutionOutcomeStatus::Planned,
        title: format!("Structured ingest plan for {}", plan.fact_type),
        summary: format!(
            "Plan {} ingests {} estimated rows from {} partition {}.",
            plan.batch_id, plan.estimated_rows, plan.source_ref, plan.partition_ref
        ),
        domain: Some("matrix".to_string()),
        refs: vec![
            CowdExecutionRef {
                ref_type: "structured_source".to_string(),
                id: plan.source_ref.clone(),
                label: Some(plan.source_ref.clone()),
            },
            CowdExecutionRef {
                ref_type: "structured_batch".to_string(),
                id: plan.batch_id.clone(),
                label: Some(plan.fact_type.clone()),
            },
        ],
        evidence_refs: Vec::new(),
        metrics: plan.affected_metric_ids.clone(),
        payload: serde_json::to_value(plan).unwrap_or(Value::Null),
        created_at: plan.planned_at,
    }
}

pub(super) fn matrix_fact_outcome(fact: &MatrixFact) -> CowdExecutionOutcome {
    let mut refs = vec![CowdExecutionRef {
        ref_type: "structured_fact".to_string(),
        id: fact.fact_id.clone(),
        label: Some(fact.fact_type.clone()),
    }];
    if let Some(source_ref) = fact.source_ref.as_ref() {
        refs.push(CowdExecutionRef {
            ref_type: "structured_source".to_string(),
            id: source_ref.clone(),
            label: Some(source_ref.clone()),
        });
    }
    CowdExecutionOutcome {
        outcome_id: format!("structured-fact:{}", fact.fact_id),
        kind: CowdExecutionOutcomeKind::StructuredFact,
        status: CowdExecutionOutcomeStatus::Succeeded,
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
        metrics: fact.metric_key.iter().cloned().collect(),
        payload: serde_json::to_value(fact).unwrap_or(Value::Null),
        created_at: fact.event_time,
    }
}

pub(super) fn matrix_evidence_packet_outcome(
    packet: &MatrixEvidencePacket,
) -> CowdExecutionOutcome {
    CowdExecutionOutcome {
        outcome_id: format!("structured-evidence:{}", packet.packet_id),
        kind: CowdExecutionOutcomeKind::StructuredEvidence,
        status: if packet.missing_evidence.is_empty() {
            CowdExecutionOutcomeStatus::Succeeded
        } else {
            CowdExecutionOutcomeStatus::Partial
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
        refs: vec![CowdExecutionRef {
            ref_type: "structured_evidence".to_string(),
            id: packet.packet_id.clone(),
            label: packet.attention_id.clone(),
        }],
        evidence_refs: packet
            .source_refs
            .iter()
            .map(|source| source.reference.clone())
            .collect(),
        metrics: packet
            .metric_evidence
            .iter()
            .filter_map(|item| item.get("metric_id").and_then(Value::as_str))
            .map(ToString::to_string)
            .collect(),
        payload: serde_json::to_value(packet).unwrap_or(Value::Null),
        created_at: packet.created_at,
    }
}

fn execution_status(status: &str) -> CowdExecutionOutcomeStatus {
    match status {
        "planned" | "dry_run_ready" | "queued_for_human_review" => {
            CowdExecutionOutcomeStatus::Planned
        }
        "running" | "cross_plane_dispatched" => CowdExecutionOutcomeStatus::Running,
        "completed" | "success" | "feedback_resolved" => CowdExecutionOutcomeStatus::Succeeded,
        "failed" | "error" | "feedback_rejected" => CowdExecutionOutcomeStatus::Failed,
        "blocked" | "cross_plane_blocked" => CowdExecutionOutcomeStatus::Blocked,
        _ => CowdExecutionOutcomeStatus::Partial,
    }
}

async fn ensure_matrix_outcome_session_record(
    state: &AppState,
    session_id: &str,
) -> Result<(), String> {
    let Some(store) = state.services.session.unified_store() else {
        return Ok(());
    };
    let now = chrono::Utc::now().to_rfc3339();
    if let Some(mut record) = store
        .get_session(session_id)
        .await
        .map_err(|error| error.to_string())?
    {
        record.last_activity = now;
        record.platform = "mfg".to_string();
        store
            .update_session(&record)
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    }
    let metadata_json = serde_json::json!({
        "kind": "mfg.execution_outcome.session",
        "session_id": session_id,
    })
    .to_string();
    let record = SessionRecord {
        session_id: session_id.to_string(),
        platform: "mfg".to_string(),
        chat_id: session_id.to_string(),
        user_id: None,
        model: None,
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(metadata_json),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    };
    store
        .create_session(&record)
        .await
        .map_err(|error| error.to_string())
}
