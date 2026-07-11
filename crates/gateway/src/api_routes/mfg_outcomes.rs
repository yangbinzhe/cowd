use app_mfg::{MfgActionExecution, MfgSkillRun};
use memory::store::session::SessionRecord;
use runtime::execution_outcome::{
    CowdExecutionOutcome, CowdExecutionOutcomeKind, CowdExecutionOutcomeStatus, CowdExecutionRef,
};

use super::AppState;

pub(super) async fn append_mfg_execution_outcome(
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
    ensure_mfg_outcome_session_record(state, session_id)
        .await
        .map_err(|error| format!("failed to prepare MFG outcome session: {error}"))?;
    let event = memory::SessionDomainEvent::new(
        session_id,
        0,
        memory::SessionDomainScope::Mfg,
        "mfg.execution_outcome",
        serde_json::to_value(&outcome).map_err(|error| error.to_string())?,
        outcome.created_at.timestamp_millis().max(0) as u64,
    );
    store
        .append_session_domain_event_allocating_sequence(&event)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub(super) fn mfg_action_execution_outcome(execution: &MfgActionExecution) -> CowdExecutionOutcome {
    CowdExecutionOutcome {
        outcome_id: format!("manufacturing-action:{}", execution.execution_id),
        kind: CowdExecutionOutcomeKind::ApplicationAction,
        status: execution_status(&execution.status),
        title: execution.title.clone(),
        summary: format!(
            "MFG action {} for incident {} is {}.",
            execution.action_id, execution.incident_id, execution.status
        ),
        domain: Some("mfg".to_string()),
        refs: vec![
            CowdExecutionRef {
                ref_type: "mfg_execution".to_string(),
                id: execution.execution_id.clone(),
                label: Some(execution.action_type.clone()),
            },
            CowdExecutionRef {
                ref_type: "mfg_incident".to_string(),
                id: execution.incident_id.clone(),
                label: None,
            },
        ],
        evidence_refs: execution
            .cross_plane_receipts
            .iter()
            .filter_map(|receipt| receipt.audit_record_id.clone())
            .collect(),
        metrics: Vec::new(),
        payload: execution.receipt.clone(),
        created_at: execution.created_at,
    }
}

pub(super) fn mfg_skill_run_execution_outcome(run: &MfgSkillRun) -> CowdExecutionOutcome {
    CowdExecutionOutcome {
        outcome_id: format!(
            "skill-run:{}",
            run.execution_id
                .clone()
                .unwrap_or_else(|| format!("{}:{}", run.incident_id, run.skill_id))
        ),
        kind: CowdExecutionOutcomeKind::SkillRun,
        status: execution_status(&run.status),
        title: format!("Skill run {}", run.skill_id),
        summary: run.summary.clone(),
        domain: Some("mfg".to_string()),
        refs: vec![
            CowdExecutionRef {
                ref_type: "mfg_skill".to_string(),
                id: run.skill_id.clone(),
                label: run.agent_node_id.clone(),
            },
            CowdExecutionRef {
                ref_type: "mfg_incident".to_string(),
                id: run.incident_id.clone(),
                label: None,
            },
        ],
        evidence_refs: run
            .execution_context
            .as_ref()
            .map(|context| context.evidence_refs.clone())
            .unwrap_or_default(),
        metrics: run
            .execution_context
            .as_ref()
            .map(|context| context.metric_keys.clone())
            .unwrap_or_default(),
        payload: run.structured_report.clone(),
        created_at: run
            .telemetry
            .as_ref()
            .map(|telemetry| telemetry.completed_at)
            .unwrap_or_else(chrono::Utc::now),
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

async fn ensure_mfg_outcome_session_record(
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
