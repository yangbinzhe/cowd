//! Runtime event-sourced recovery planner and executor.

use serde::{Deserialize, Serialize};

use crate::{
    candidate_from_action, RuntimeEventInput, RuntimeEventRef, RuntimeEventReplayer,
    RuntimeEventScope, RuntimeRecoveryAction, RuntimeRecoveryActionKind, RuntimeRecoveryCandidate,
    RuntimeReplayReport, RuntimeServices,
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
    pub fn plan(limit: usize, services: &RuntimeServices) -> Result<RecoveryPlan, String> {
        let mut report = RuntimeEventReplayer::report(services.event_store(), limit)?;
        let candidates = augmented_candidates(&report, services);
        report.candidates = candidates.clone();
        Ok(RecoveryPlan {
            kind: "runtime.recovery_plan".to_string(),
            actions: report.actions.clone(),
            candidates,
            report,
        })
    }
}

fn augmented_candidates(
    report: &RuntimeReplayReport,
    services: &RuntimeServices,
) -> Vec<RuntimeRecoveryCandidate> {
    let mut candidates = report
        .actions
        .iter()
        .filter_map(candidate_from_action)
        .collect::<Vec<_>>();
    for agent in services.agent_runtime().list() {
        let (action, risk, precondition) = match agent.status {
            harness_contract::agent::AgentStatus::Prepared
            | harness_contract::agent::AgentStatus::Starting
            | harness_contract::agent::AgentStatus::Running
            | harness_contract::agent::AgentStatus::WaitingInput
            | harness_contract::agent::AgentStatus::WaitingApproval
            | harness_contract::agent::AgentStatus::Paused => (
                RuntimeRecoveryActionKind::MarkInterrupted,
                "medium",
                "agent run is not terminal and must be confirmed before retry or synthesis",
            ),
            harness_contract::agent::AgentStatus::Blocked
            | harness_contract::agent::AgentStatus::Completed
            | harness_contract::agent::AgentStatus::Failed
            | harness_contract::agent::AgentStatus::Cancelled => continue,
        };
        candidates.push(RuntimeRecoveryCandidate {
            candidate_id: format!("recovery-candidate-agent-{}", agent.agent_id),
            owner: "runtime.agent".to_string(),
            source_stream_id: format!("agent:{}", agent.agent_id),
            scope: RuntimeEventScope::Agent,
            action,
            risk: risk.to_string(),
            precondition: precondition.to_string(),
            reason: format!("agent status is {:?}", agent.status).to_ascii_lowercase(),
            evidence_refs: vec![format!("graph:{}", agent.graph_id)],
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
    pub fn execute(
        limit: usize,
        services: &RuntimeServices,
    ) -> Result<RecoveryExecutionReport, String> {
        let plan = RecoveryPlanner::plan(limit, services)?;
        let mut applied = Vec::new();
        let mut skipped = Vec::new();
        let failed = Vec::<RecoveryFailedAction>::new();

        let actions = executable_recovery_actions(&plan);
        for action in &actions {
            match apply_action(action, services) {
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
        record_recovery_event(&report, services);
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
}

fn apply_action(
    action: &RuntimeRecoveryAction,
    _services: &RuntimeServices,
) -> RecoveryApplyOutcome {
    match action.action {
        RuntimeRecoveryActionKind::PreservePending => {
            RecoveryApplyOutcome::Applied("pending work preserved".to_string())
        }
        RuntimeRecoveryActionKind::ReplayOnly => {
            RecoveryApplyOutcome::Skipped("replay-only stream".to_string())
        }
        RuntimeRecoveryActionKind::MarkInterrupted if action.stream_id.starts_with("team:") => {
            RecoveryApplyOutcome::Skipped(
                "team recovery is derived from the durable ExecutionGraph; no mutable team state exists"
                    .to_string(),
            )
        }
        RuntimeRecoveryActionKind::MarkInterrupted if action.stream_id.starts_with("agent:") => {
            RecoveryApplyOutcome::Skipped(
                "AgentRuntime interruption is delivered through the async RuntimeHost command adapter"
                    .to_string(),
            )
        }
        _ => RecoveryApplyOutcome::Skipped("no safe executor for stream/action".to_string()),
    }
}

fn record_recovery_event(report: &RecoveryExecutionReport, services: &RuntimeServices) {
    let _ = services.event_store().append(RuntimeEventInput {
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
