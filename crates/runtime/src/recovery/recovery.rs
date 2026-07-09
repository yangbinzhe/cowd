//! Runtime event-sourced recovery planner and executor.

use serde::{Deserialize, Serialize};

use crate::{
    candidate_from_action, global_agent_lifecycle_service, global_agent_task_mailbox,
    global_mission_runtime, global_runtime_event_store, global_steward_runtime_service,
    global_team_runtime_service, record_runtime_event, AgentExecutionCommandKind, AgentTaskStatus,
    MissionSessionCommandStatus, RuntimeEventInput, RuntimeEventRef, RuntimeEventReplayer,
    RuntimeEventScope, RuntimeRecoveryAction, RuntimeRecoveryActionKind, RuntimeRecoveryCandidate,
    RuntimeReplayReport,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryPlan {
    pub kind: String,
    pub report: RuntimeReplayReport,
    pub actions: Vec<RuntimeRecoveryAction>,
    pub candidates: Vec<RuntimeRecoveryCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryExecutionReport {
    pub kind: String,
    pub ok: bool,
    pub applied: Vec<RecoveryAppliedAction>,
    pub skipped: Vec<RecoverySkippedAction>,
    pub failed: Vec<RecoveryFailedAction>,
    pub plan: RecoveryPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryAppliedAction {
    pub stream_id: String,
    pub action: RuntimeRecoveryActionKind,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverySkippedAction {
    pub stream_id: String,
    pub action: RuntimeRecoveryActionKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryFailedAction {
    pub stream_id: String,
    pub action: RuntimeRecoveryActionKind,
    pub error: String,
}

#[derive(Debug, Default)]
pub struct RecoveryPlanner;

impl RecoveryPlanner {
    pub fn plan(limit: usize) -> Result<RecoveryPlan, String> {
        let mut report = RuntimeEventReplayer::report(global_runtime_event_store(), limit)?;
        let candidates = augmented_candidates(&report);
        report.candidates = candidates.clone();
        Ok(RecoveryPlan {
            kind: "runtime.recovery_plan".to_string(),
            actions: report.actions.clone(),
            candidates,
            report,
        })
    }
}

fn augmented_candidates(report: &RuntimeReplayReport) -> Vec<RuntimeRecoveryCandidate> {
    let mut candidates = report
        .actions
        .iter()
        .filter_map(candidate_from_action)
        .collect::<Vec<_>>();
    for command in global_mission_runtime().projection().session_commands {
        let (action, risk, precondition) = match command.status {
            MissionSessionCommandStatus::Pending => (
                RuntimeRecoveryActionKind::PreservePending,
                "low",
                "pending command must remain queued until dispatch policy accepts it",
            ),
            MissionSessionCommandStatus::Claimed | MissionSessionCommandStatus::Running => (
                RuntimeRecoveryActionKind::MarkInterrupted,
                "medium",
                "claimed or running session command needs explicit recovery before retry",
            ),
            MissionSessionCommandStatus::Failed => (
                RuntimeRecoveryActionKind::MarkInterrupted,
                "medium",
                "failed session command can be retried only after operator or steward review",
            ),
            MissionSessionCommandStatus::Completed
            | MissionSessionCommandStatus::Cancelled
            | MissionSessionCommandStatus::Interrupted => continue,
        };
        candidates.push(RuntimeRecoveryCandidate {
            candidate_id: format!("recovery-candidate-session-command-{}", command.command_id),
            owner: "runtime.session".to_string(),
            source_stream_id: format!("session-command:{}", command.command_id),
            scope: RuntimeEventScope::SessionCommand,
            action,
            risk: risk.to_string(),
            precondition: precondition.to_string(),
            reason: command.error.clone().unwrap_or_else(|| {
                format!("session command status is {}", command.status.as_str())
            }),
            evidence_refs: command.evidence_refs.clone(),
        });
    }
    for task in global_agent_task_mailbox().list() {
        let (action, risk, precondition) = match task.status {
            AgentTaskStatus::Pending | AgentTaskStatus::Claimed | AgentTaskStatus::Running => (
                RuntimeRecoveryActionKind::MarkInterrupted,
                "medium",
                "agent task is not terminal and must be confirmed before retry or synthesis",
            ),
            AgentTaskStatus::Failed => (
                RuntimeRecoveryActionKind::MarkInterrupted,
                "medium",
                "failed agent task needs retry, cancellation, or synthesis exclusion",
            ),
            AgentTaskStatus::Completed | AgentTaskStatus::Cancelled => continue,
        };
        candidates.push(RuntimeRecoveryCandidate {
            candidate_id: format!("recovery-candidate-agent-task-{}", task.task_id),
            owner: "runtime.agent_task_mailbox".to_string(),
            source_stream_id: format!("agent-task:{}", task.task_id),
            scope: RuntimeEventScope::Agent,
            action,
            risk: risk.to_string(),
            precondition: precondition.to_string(),
            reason: format!("agent task status is {:?}", task.status).to_ascii_lowercase(),
            evidence_refs: task.evidence_refs.clone(),
        });
    }
    dedupe_candidates(candidates)
}

fn dedupe_candidates(candidates: Vec<RuntimeRecoveryCandidate>) -> Vec<RuntimeRecoveryCandidate> {
    let mut seen = std::collections::BTreeSet::new();
    candidates
        .into_iter()
        .filter(|candidate| {
            seen.insert((
                candidate.source_stream_id.clone(),
                format!("{:?}", candidate.action),
            ))
        })
        .collect()
}

#[derive(Debug, Default)]
pub struct RecoveryExecutor;

impl RecoveryExecutor {
    pub fn execute(limit: usize) -> Result<RecoveryExecutionReport, String> {
        let plan = RecoveryPlanner::plan(limit)?;
        let mut applied = Vec::new();
        let mut skipped = Vec::new();
        let mut failed = Vec::new();

        let actions = executable_recovery_actions(&plan);
        for action in &actions {
            match apply_action(action) {
                RecoveryApplyOutcome::Applied(summary) => {
                    applied.push(RecoveryAppliedAction {
                        stream_id: action.stream_id.clone(),
                        action: action.action.clone(),
                        summary,
                    });
                }
                RecoveryApplyOutcome::Skipped(reason) => {
                    skipped.push(RecoverySkippedAction {
                        stream_id: action.stream_id.clone(),
                        action: action.action.clone(),
                        reason,
                    });
                }
                RecoveryApplyOutcome::Failed(error) => {
                    failed.push(RecoveryFailedAction {
                        stream_id: action.stream_id.clone(),
                        action: action.action.clone(),
                        error,
                    });
                }
            }
        }

        let report = RecoveryExecutionReport {
            kind: "runtime.recovery_execution_report".to_string(),
            ok: failed.is_empty(),
            applied,
            skipped,
            failed,
            plan,
        };
        record_recovery_event(&report);
        Ok(report)
    }
}

fn executable_recovery_actions(plan: &RecoveryPlan) -> Vec<RuntimeRecoveryAction> {
    let mut seen = std::collections::BTreeSet::new();
    let mut actions = Vec::new();

    for action in &plan.actions {
        if seen.insert((action.stream_id.clone(), format!("{:?}", action.action))) {
            actions.push(action.clone());
        }
    }

    for candidate in &plan.candidates {
        if seen.insert((
            candidate.source_stream_id.clone(),
            format!("{:?}", candidate.action),
        )) {
            actions.push(RuntimeRecoveryAction {
                stream_id: candidate.source_stream_id.clone(),
                scope: candidate.scope,
                latest_kind: format!("recovery.candidate.{}", candidate.owner),
                latest_status: None,
                action: candidate.action.clone(),
                reason: candidate.reason.clone(),
            });
        }
    }

    actions
}

enum RecoveryApplyOutcome {
    Applied(String),
    Skipped(String),
    Failed(String),
}

fn apply_action(action: &RuntimeRecoveryAction) -> RecoveryApplyOutcome {
    match action.action {
        RuntimeRecoveryActionKind::PreservePending => {
            RecoveryApplyOutcome::Applied("pending work preserved".to_string())
        }
        RuntimeRecoveryActionKind::ReplayOnly => {
            RecoveryApplyOutcome::Skipped("replay-only stream".to_string())
        }
        RuntimeRecoveryActionKind::PauseRecoveryRequired
            if action.stream_id.starts_with("steward:") =>
        {
            let steward_id = action.stream_id.trim_start_matches("steward:");
            match global_steward_runtime_service()
                .mark_recovery_required(steward_id, action.reason.clone())
            {
                Ok(_) => RecoveryApplyOutcome::Applied("steward paused for recovery".to_string()),
                Err(error) => RecoveryApplyOutcome::Failed(error),
            }
        }
        RuntimeRecoveryActionKind::MarkInterrupted
            if action.stream_id.starts_with("session-command:") =>
        {
            let command_id = action.stream_id.trim_start_matches("session-command:");
            match global_mission_runtime().get_session_command(command_id) {
                Some(command) => match global_mission_runtime().interrupt_session_command(
                    &command.target_session_id,
                    command_id,
                    action.reason.clone(),
                ) {
                    Ok(_) => RecoveryApplyOutcome::Applied(
                        "session command marked interrupted".to_string(),
                    ),
                    Err(error) => RecoveryApplyOutcome::Failed(error),
                },
                None => RecoveryApplyOutcome::Skipped("session command not found".to_string()),
            }
        }
        RuntimeRecoveryActionKind::MarkInterrupted if action.stream_id.starts_with("team:") => {
            let team_id = action.stream_id.trim_start_matches("team:");
            match global_team_runtime_service()
                .request_review(team_id, "recovery required after interrupted runtime event")
            {
                Ok(_) => RecoveryApplyOutcome::Applied("team marked review_required".to_string()),
                Err(error) => RecoveryApplyOutcome::Failed(error),
            }
        }
        RuntimeRecoveryActionKind::MarkInterrupted if action.stream_id.starts_with("agent:") => {
            let agent_id = action.stream_id.trim_start_matches("agent:");
            match global_agent_lifecycle_service().command(
                agent_id,
                AgentExecutionCommandKind::Interrupt,
                Some(serde_json::json!({
                    "reason": action.reason,
                })),
            ) {
                Ok(_) => RecoveryApplyOutcome::Applied("agent interrupted".to_string()),
                Err(error) => RecoveryApplyOutcome::Failed(error),
            }
        }
        _ => RecoveryApplyOutcome::Skipped("no safe executor for stream/action".to_string()),
    }
}

fn record_recovery_event(report: &RecoveryExecutionReport) {
    let _ = record_runtime_event(RuntimeEventInput {
        stream_id: "runtime:recovery".to_string(),
        scope: RuntimeEventScope::Recovery,
        kind: "runtime.recovery.executed".to_string(),
        status: Some(if report.ok { "ok" } else { "failed" }.to_string()),
        actor: Some("recovery_executor".to_string()),
        refs: report
            .applied
            .iter()
            .map(|action| RuntimeEventRef {
                kind: "stream".to_string(),
                id: action.stream_id.clone(),
            })
            .collect(),
        payload: serde_json::json!(report),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StartMissionSessionRequest, StartStewardRuntimeRequest};

    #[test]
    fn recovery_executor_marks_session_command_and_steward() {
        let _guard = crate::test_env_lock();
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("recovery-session-{suffix}");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "recovery session".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("session");
        let command = global_mission_runtime()
            .enqueue_session_command(&session_id, &session_id, "recover me")
            .expect("command");
        global_mission_runtime()
            .mark_session_command_running(&session_id, &command.command_id)
            .expect("running");
        global_runtime_event_store()
            .append(RuntimeEventInput {
                stream_id: format!("session-command:{}", command.command_id),
                scope: RuntimeEventScope::SessionCommand,
                kind: "mission.session.command_running".to_string(),
                status: Some("running".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .expect("append command event");
        let steward = global_steward_runtime_service()
            .start(StartStewardRuntimeRequest {
                mission_id: "recovery-test".to_string(),
                root_session_id: Some(session_id.clone()),
                profile_id: crate::AutonomyProfileId::Stewarded,
                objective: "recover steward".to_string(),
            })
            .expect("steward");
        global_runtime_event_store()
            .append(RuntimeEventInput {
                stream_id: format!("steward:{}", steward.steward_id),
                scope: RuntimeEventScope::Steward,
                kind: "steward.started".to_string(),
                status: Some("running".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .expect("append steward event");

        let report = RecoveryExecutor::execute(1_000).expect("recover");
        assert!(report
            .plan
            .candidates
            .iter()
            .any(|candidate| candidate.owner == "runtime.session"
                && candidate
                    .source_stream_id
                    .contains(command.command_id.as_str())));
        assert!(report
            .plan
            .candidates
            .iter()
            .any(|candidate| candidate.owner == "runtime.steward_runtime"
                && candidate
                    .source_stream_id
                    .contains(steward.steward_id.as_str())));
        assert!(report.applied.iter().any(|action| {
            action.stream_id == format!("session-command:{}", command.command_id)
        }));
        assert!(report
            .applied
            .iter()
            .any(|action| action.stream_id == format!("steward:{}", steward.steward_id)));
        assert_eq!(
            global_mission_runtime()
                .get_session_command(&command.command_id)
                .expect("command after")
                .status,
            crate::MissionSessionCommandStatus::Interrupted
        );
    }
}
