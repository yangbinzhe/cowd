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
pub struct RuntimeReplayReport {
    pub kind: String,
    pub total_events: usize,
    pub scope_counts: BTreeMap<String, usize>,
    pub actions: Vec<RuntimeRecoveryAction>,
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

        Ok(RuntimeReplayReport {
            kind: "runtime.replay_report".to_string(),
            total_events: scope_counts.values().sum(),
            scope_counts,
            actions,
            recovery_required,
        })
    }
}

fn recovery_action(scope: RuntimeEventScope, status: &str) -> (RuntimeRecoveryActionKind, String) {
    match (scope, status) {
        (RuntimeEventScope::Approval | RuntimeEventScope::SessionCommand, "pending") => (
            RuntimeRecoveryActionKind::PreservePending,
            "pending work must survive restart".to_string(),
        ),
        (RuntimeEventScope::Steward, "running" | "waiting_dependency" | "waiting_approval") => (
            RuntimeRecoveryActionKind::PauseRecoveryRequired,
            "steward must pause for recovery review after restart".to_string(),
        ),
        (
            RuntimeEventScope::Team | RuntimeEventScope::Agent | RuntimeEventScope::SessionCommand,
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
        store
            .append(RuntimeEventInput {
                stream_id: "steward:s".to_string(),
                scope: RuntimeEventScope::Steward,
                kind: "steward.started".to_string(),
                status: Some("running".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::<RuntimeEventRef>::new(),
                payload: serde_json::json!({}),
            })
            .expect("steward append");

        let report = RuntimeEventReplayer::report(&store, 100).expect("report");
        assert_eq!(report.total_events, 2);
        assert!(report.actions.iter().any(|action| {
            action.stream_id == "approval:a"
                && action.action == RuntimeRecoveryActionKind::PreservePending
        }));
        assert!(report.actions.iter().any(|action| {
            action.stream_id == "steward:s"
                && action.action == RuntimeRecoveryActionKind::PauseRecoveryRequired
        }));
    }
}
