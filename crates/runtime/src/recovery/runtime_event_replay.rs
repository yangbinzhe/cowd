//! Runtime event replay and recovery reporting.
//!
//! This module turns the durable runtime event ledger into an explicit recovery
//! surface. It identifies streams that require safe human-visible recovery
//! actions and preserves pending work.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{DurableRuntimeEvent, RuntimeEventScope, RuntimeEventStore};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeRecoveryActionKind {
    PreservePending,
    MarkInterrupted,
    PauseRecoveryRequired,
    ReplayOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeRecoveryAction {
    pub stream_id: String,
    pub scope: RuntimeEventScope,
    pub latest_kind: String,
    pub latest_status: Option<String>,
    pub action: RuntimeRecoveryActionKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeRecoveryCandidate {
    pub candidate_id: String,
    pub owner: String,
    pub source_stream_id: String,
    pub scope: RuntimeEventScope,
    pub action: RuntimeRecoveryActionKind,
    pub risk: String,
    pub precondition: String,
    pub reason: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeReplayReport {
    pub kind: String,
    pub total_events: usize,
    pub scope_counts: BTreeMap<String, usize>,
    pub actions: Vec<RuntimeRecoveryAction>,
    pub candidates: Vec<RuntimeRecoveryCandidate>,
    pub recovery_required: usize,
}

#[derive(Debug, Default)]
pub struct RuntimeEventReplayer;

impl RuntimeEventReplayer {
    pub fn report(store: &RuntimeEventStore, limit: usize) -> Result<RuntimeReplayReport, String> {
        let events = store.all_events(limit)?;
        let mut scope_counts = BTreeMap::new();
        let mut latest_by_stream: BTreeMap<String, DurableRuntimeEvent> = BTreeMap::new();

        for event in events {
            *scope_counts
                .entry(event.scope.as_str().to_string())
                .or_insert(0) += 1;
            latest_by_stream
                .entry(event.stream_id.clone())
                .and_modify(|current| {
                    if event.sequence > current.sequence {
                        *current = event.clone();
                    }
                })
                .or_insert(event);
        }

        let mut actions = latest_by_stream
            .values()
            .map(|event| {
                let status = event.status.as_deref().unwrap_or("");
                let (action, reason) = recovery_action(event.scope, status);
                RuntimeRecoveryAction {
                    stream_id: event.stream_id.clone(),
                    scope: event.scope,
                    latest_kind: event.kind.clone(),
                    latest_status: event.status.clone(),
                    action,
                    reason,
                }
            })
            .collect::<Vec<_>>();
        actions.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
        let recovery_required = actions
            .iter()
            .filter(|action| {
                !matches!(
                    action.action,
                    RuntimeRecoveryActionKind::ReplayOnly
                        | RuntimeRecoveryActionKind::PreservePending
                )
            })
            .count();
        let candidates = actions
            .iter()
            .filter_map(candidate_from_action)
            .collect::<Vec<_>>();

        Ok(RuntimeReplayReport {
            kind: "runtime.replay_report".to_string(),
            total_events: scope_counts.values().sum(),
            scope_counts,
            actions,
            candidates,
            recovery_required,
        })
    }
}

#[must_use]
pub fn candidate_from_action(action: &RuntimeRecoveryAction) -> Option<RuntimeRecoveryCandidate> {
    let owner = match action.scope {
        RuntimeEventScope::ExecutionGraph | RuntimeEventScope::ExecutionNode => {
            "runtime.execution_graph"
        }
        RuntimeEventScope::Goal => "runtime.goal_runtime",
        RuntimeEventScope::Session
        | RuntimeEventScope::SessionInput
        | RuntimeEventScope::SessionCommand => "runtime.session",
        RuntimeEventScope::Team => "runtime.team_projection",
        RuntimeEventScope::Agent => "runtime.agent_lifecycle",
        RuntimeEventScope::Approval => "runtime.approval_queue",
        // Steward events from prior builds have no active lifecycle owner.
        // They remain replayable evidence only and can never restart work.
        RuntimeEventScope::Steward => "runtime.agent_policy",
        RuntimeEventScope::Tool => "runtime.tool_host",
        RuntimeEventScope::Recovery => "runtime.recovery",
        RuntimeEventScope::CrossPlane => "runtime.cross_plane",
        RuntimeEventScope::Mission
        | RuntimeEventScope::Relation
        | RuntimeEventScope::Task
        | RuntimeEventScope::Worker
        | RuntimeEventScope::Schedule => "runtime.mission_control",
    }
    .to_string();
    let (risk, precondition) = match action.action {
        RuntimeRecoveryActionKind::PreservePending => (
            "low",
            "preserve pending state; do not auto-complete without owner confirmation",
        ),
        RuntimeRecoveryActionKind::ReplayOnly => (
            "low",
            "replay event stream only; no state mutation required",
        ),
        RuntimeRecoveryActionKind::MarkInterrupted => (
            "medium",
            "latest stream status is running or claimed and cannot be assumed complete",
        ),
        RuntimeRecoveryActionKind::PauseRecoveryRequired => (
            "high",
            "running autonomous work must be paused before human or steward review",
        ),
    };
    Some(RuntimeRecoveryCandidate {
        candidate_id: format!(
            "recovery-candidate-{}",
            stable_id(&format!(
                "{}:{}:{:?}",
                action.stream_id, action.latest_kind, action.action
            ))
        ),
        owner,
        source_stream_id: action.stream_id.clone(),
        scope: action.scope,
        action: action.action.clone(),
        risk: risk.to_string(),
        precondition: precondition.to_string(),
        reason: action.reason.clone(),
        evidence_refs: vec![format!("runtime-stream:{}", action.stream_id)],
    })
}

fn recovery_action(scope: RuntimeEventScope, status: &str) -> (RuntimeRecoveryActionKind, String) {
    match (scope, status) {
        (RuntimeEventScope::Approval | RuntimeEventScope::SessionInput, "pending") => (
            RuntimeRecoveryActionKind::PreservePending,
            "pending work must survive restart".to_string(),
        ),
        (
            RuntimeEventScope::ExecutionGraph
            | RuntimeEventScope::ExecutionNode
            | RuntimeEventScope::Goal
            | RuntimeEventScope::Team
            | RuntimeEventScope::Agent
            | RuntimeEventScope::SessionInput,
            "running" | "claimed",
        ) => (
            RuntimeRecoveryActionKind::MarkInterrupted,
            "running work cannot be assumed complete after restart".to_string(),
        ),
        _ => (
            RuntimeRecoveryActionKind::ReplayOnly,
            "event stream can be replayed for projection evidence".to_string(),
        ),
    }
}

fn stable_id(input: &str) -> String {
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RuntimeEventInput, RuntimeEventRef};

    #[test]
    fn replay_report_preserves_pending_and_marks_running_recovery() {
        let store = RuntimeEventStore::open_in_memory().expect("store");
        store
            .append(RuntimeEventInput {
                stream_id: "approval:a".to_string(),
                scope: RuntimeEventScope::Approval,
                kind: "approval.submitted".to_string(),
                status: Some("pending".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::<RuntimeEventRef>::new(),
                payload: serde_json::json!({}),
            })
            .expect("approval append");
        let report = RuntimeEventReplayer::report(&store, 100).expect("report");
        assert_eq!(report.total_events, 1);
        assert!(report.candidates.iter().any(|candidate| {
            candidate.owner == "runtime.approval_queue"
                && candidate.action == RuntimeRecoveryActionKind::PreservePending
        }));
        assert!(report.actions.iter().any(|action| {
            action.stream_id == "approval:a"
                && action.action == RuntimeRecoveryActionKind::PreservePending
        }));
    }
}
