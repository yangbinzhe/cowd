//! Bridge terminal agent lifecycle events back into runtime team execution.

use serde::{Deserialize, Serialize};

use crate::{
    global_agent_event_bus, global_agent_task_binding_registry, global_agent_task_mailbox,
    global_mission_evidence_bus, global_team_runtime_service, AgentProgressEvent, AgentSnapshot,
    AgentTaskCompletionReceipt, AgentTaskOutcome, AgentTaskQualityStatus, AgentTaskStatus,
    MissionEvidenceRef,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRuntimeOutcomeBridgeReceipt {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub terminal_status: AgentTaskStatus,
    pub applied: bool,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_receipt: Option<AgentTaskCompletionReceipt>,
}

pub fn bridge_agent_terminal_snapshot(
    snapshot: &AgentSnapshot,
    event_type: &str,
    message: impl Into<String>,
) -> Option<AgentRuntimeOutcomeBridgeReceipt> {
    let terminal_status = terminal_task_status(snapshot.status.as_str(), event_type)?;
    let message = message.into();
    let Some(binding) = global_agent_task_binding_registry().get_by_agent(&snapshot.agent_id)
    else {
        return Some(AgentRuntimeOutcomeBridgeReceipt {
            agent_id: snapshot.agent_id.clone(),
            team_id: None,
            task_id: None,
            terminal_status,
            applied: false,
            message: "terminal agent event has no active team task binding".to_string(),
            completion_receipt: None,
        });
    };

    let task = match global_agent_task_mailbox().get(&binding.task_id) {
        Some(task) => task,
        None => {
            return Some(AgentRuntimeOutcomeBridgeReceipt {
                agent_id: snapshot.agent_id.clone(),
                team_id: Some(binding.team_id),
                task_id: Some(binding.task_id),
                terminal_status,
                applied: false,
                message: "terminal agent event binding points to missing task".to_string(),
                completion_receipt: None,
            });
        }
    };
    if is_terminal_task_status(task.status) {
        global_agent_task_binding_registry().mark_task_status(&snapshot.agent_id, task.status);
        return Some(AgentRuntimeOutcomeBridgeReceipt {
            agent_id: snapshot.agent_id.clone(),
            team_id: Some(task.team_id),
            task_id: Some(task.task_id),
            terminal_status,
            applied: false,
            message: "agent task was already terminal".to_string(),
            completion_receipt: None,
        });
    }

    let receipt = match terminal_status {
        AgentTaskStatus::Completed => {
            let outcome = AgentTaskOutcome {
                result_summary: terminal_result_summary(snapshot, &message),
                evidence_refs: terminal_evidence_refs(snapshot, &binding.workgraph_node_id),
                conflicts: Vec::new(),
                suggested_next_actions: vec!["synthesize".to_string()],
                quality_status: AgentTaskQualityStatus::Accepted,
                completed_at_ms: 0,
            };
            global_agent_task_mailbox()
                .complete(&binding.task_id, outcome)
                .ok()?
        }
        AgentTaskStatus::Failed => global_agent_task_mailbox()
            .fail(
                &binding.task_id,
                terminal_result_summary(snapshot, &message),
                terminal_evidence_refs(snapshot, &binding.workgraph_node_id),
                snapshot.error.clone().into_iter().collect(),
            )
            .ok()?,
        AgentTaskStatus::Cancelled => global_agent_task_mailbox()
            .cancel(
                &binding.task_id,
                terminal_result_summary(snapshot, &message),
            )
            .ok()?,
        AgentTaskStatus::Pending | AgentTaskStatus::Claimed | AgentTaskStatus::Running => {
            return None;
        }
    };

    global_agent_task_binding_registry().mark_task_status(&snapshot.agent_id, receipt.status);
    let _ = global_team_runtime_service().apply_agent_task_outcome(&receipt);
    record_bridge_progress(snapshot, &receipt);
    Some(AgentRuntimeOutcomeBridgeReceipt {
        agent_id: snapshot.agent_id.clone(),
        team_id: Some(receipt.team_id.clone()),
        task_id: Some(receipt.task_id.clone()),
        terminal_status,
        applied: true,
        message: "terminal agent event applied to team task".to_string(),
        completion_receipt: Some(receipt),
    })
}

fn terminal_task_status(status: &str, event_type: &str) -> Option<AgentTaskStatus> {
    let status = status.trim().to_ascii_lowercase();
    match (status.as_str(), event_type) {
        ("completed", _) | (_, "agent.completed") => Some(AgentTaskStatus::Completed),
        ("failed", _) | (_, "agent.failed") => Some(AgentTaskStatus::Failed),
        ("cancelled", _) | ("canceled", _) | (_, "agent.cancelled") | (_, "agent.canceled") => {
            Some(AgentTaskStatus::Cancelled)
        }
        _ => None,
    }
}

fn is_terminal_task_status(status: AgentTaskStatus) -> bool {
    matches!(
        status,
        AgentTaskStatus::Completed | AgentTaskStatus::Failed | AgentTaskStatus::Cancelled
    )
}

fn terminal_result_summary(snapshot: &AgentSnapshot, fallback: &str) -> String {
    let derived = snapshot.derived_state.trim();
    if !derived.is_empty() {
        return derived.chars().take(4000).collect();
    }
    if !snapshot.output_file.trim().is_empty() {
        if let Ok(output) = std::fs::read_to_string(&snapshot.output_file) {
            let trimmed = output.trim();
            if !trimmed.is_empty() {
                return trimmed.chars().take(4000).collect();
            }
        }
    }
    snapshot
        .error
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn terminal_evidence_refs(snapshot: &AgentSnapshot, workgraph_node_id: &str) -> Vec<String> {
    let mut refs = vec![
        format!("agent:{}", snapshot.agent_id),
        format!("workgraph_node:{workgraph_node_id}"),
    ];
    if !snapshot.output_file.trim().is_empty() {
        refs.push(format!("agent_output:{}", snapshot.output_file));
    }
    if !snapshot.manifest_file.trim().is_empty() {
        refs.push(format!("agent_manifest:{}", snapshot.manifest_file));
    }
    refs.sort();
    refs.dedup();
    refs
}

fn record_bridge_progress(snapshot: &AgentSnapshot, receipt: &AgentTaskCompletionReceipt) {
    let summary = receipt
        .outcome
        .as_ref()
        .map(|outcome| outcome.result_summary.clone())
        .unwrap_or_else(|| receipt.message.clone());
    let progress = global_agent_event_bus().push(AgentProgressEvent {
        event_id: String::new(),
        team_id: receipt.team_id.clone(),
        session_id: receipt.session_id.clone(),
        agent_id: Some(snapshot.agent_id.clone()),
        role_id: receipt.role_id.clone(),
        task_id: Some(receipt.task_id.clone()),
        event_type: format!("agent.task.{}", task_status_label(receipt.status)),
        message: summary.clone(),
        evidence_refs: receipt.evidence_refs.clone(),
        created_at_ms: 0,
    });
    let _ = global_mission_evidence_bus().record(MissionEvidenceRef {
        evidence_id: String::new(),
        mission_id: Some("mission-control".to_string()),
        session_id: receipt.session_id.clone(),
        team_id: Some(receipt.team_id.clone()),
        agent_id: Some(snapshot.agent_id.clone()),
        kind: "agent_task_outcome".to_string(),
        summary: progress.message,
        source_ref: Some(receipt.task_id.clone()),
        created_at_ms: 0,
    });
}

fn task_status_label(status: AgentTaskStatus) -> &'static str {
    match status {
        AgentTaskStatus::Pending => "pending",
        AgentTaskStatus::Claimed => "claimed",
        AgentTaskStatus::Running => "running",
        AgentTaskStatus::Completed => "completed",
        AgentTaskStatus::Failed => "failed",
        AgentTaskStatus::Cancelled => "cancelled",
    }
}
