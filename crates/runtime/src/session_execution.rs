//! Multi-session execution plane.
//!
//! This module gives Mission Runtime an explicit dispatcher and cross-session
//! bridge. It deliberately does not pretend to run provider turns; it claims or
//! routes control-plane work and records auditable receipts for later Team/Agent
//! execution stages.

use serde::{Deserialize, Serialize};

use crate::{
    global_mission_runtime, global_session_relation_graph, record_runtime_event,
    MissionSessionCommand, MissionSessionCommandStatus, MissionSessionStatus, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope, SessionRouteCommand, SessionRouteReceipt,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionExecutionPolicy {
    pub max_commands: usize,
    pub dispatch_mode: SessionDispatchMode,
    pub allow_background: bool,
}

impl Default for SessionExecutionPolicy {
    fn default() -> Self {
        Self {
            max_commands: 10,
            dispatch_mode: SessionDispatchMode::MarkClaimedOnly,
            allow_background: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDispatchMode {
    MarkClaimedOnly,
    ControlDispatchComplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionExecutionReport {
    pub kind: String,
    pub policy: SessionExecutionPolicy,
    pub inspected: usize,
    pub dispatched: Vec<SessionCommandDispatchReceipt>,
    pub skipped: Vec<SessionExecutionSkip>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCommandDispatchReceipt {
    pub command_id: String,
    pub session_id: String,
    pub status_before: MissionSessionCommandStatus,
    pub status_after: MissionSessionCommandStatus,
    pub mode: SessionDispatchMode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionExecutionSkip {
    pub session_id: String,
    pub command_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossSessionMessage {
    pub from_session_id: String,
    pub target_ref: String,
    pub command: String,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossSessionBridgeReceipt {
    pub kind: String,
    pub status: String,
    pub route: SessionRouteReceipt,
    pub command: Option<MissionSessionCommand>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLeaseState {
    pub session_id: String,
    pub dispatchable: bool,
    pub reason: String,
}

#[derive(Debug, Default)]
pub struct SessionExecutionPlane;

impl SessionExecutionPlane {
    pub fn dispatch_pending(policy: SessionExecutionPolicy) -> SessionExecutionReport {
        let mission = global_mission_runtime().projection();
        let mut inspected = 0usize;
        let mut dispatched = Vec::new();
        let mut skipped = Vec::new();
        let mut errors = Vec::new();

        for command in mission
            .session_commands
            .iter()
            .filter(|command| command.status == MissionSessionCommandStatus::Pending)
        {
            if dispatched.len() >= policy.max_commands {
                skipped.push(SessionExecutionSkip {
                    session_id: command.target_session_id.clone(),
                    command_id: Some(command.command_id.clone()),
                    reason: "dispatch budget exhausted".to_string(),
                });
                continue;
            }
            inspected = inspected.saturating_add(1);
            let lease = lease_state(&command.target_session_id, policy.allow_background);
            if !lease.dispatchable {
                skipped.push(SessionExecutionSkip {
                    session_id: command.target_session_id.clone(),
                    command_id: Some(command.command_id.clone()),
                    reason: lease.reason,
                });
                continue;
            }
            match dispatch_command(command, policy.dispatch_mode) {
                Ok(receipt) => {
                    record_dispatch_event(&receipt);
                    dispatched.push(receipt);
                }
                Err(error) => errors.push(error),
            }
        }

        SessionExecutionReport {
            kind: "runtime.session_execution_report".to_string(),
            policy,
            inspected,
            dispatched,
            skipped,
            errors,
        }
    }

    pub fn bridge(message: CrossSessionMessage) -> CrossSessionBridgeReceipt {
        let route = global_session_relation_graph().route(SessionRouteCommand {
            from_session_id: message.from_session_id.clone(),
            target_ref: message.target_ref.clone(),
            command: message.command.clone(),
        });
        let (status, command, bridge_message) =
            if let Some(target_session_id) = route.resolved_session_id.clone() {
                match global_mission_runtime().enqueue_session_command(
                    &message.from_session_id,
                    &target_session_id,
                    message.command.clone(),
                ) {
                    Ok(command) => (
                        "routed".to_string(),
                        Some(command),
                        "cross-session command enqueued".to_string(),
                    ),
                    Err(error) => ("failed".to_string(), None, error),
                }
            } else if route.resolved_agent_id.is_some() {
                (
                    "deferred".to_string(),
                    None,
                    "agent target routing belongs to TeamExecutionLoop stage".to_string(),
                )
            } else {
                ("rejected".to_string(), None, route.message.clone())
            };
        let receipt = CrossSessionBridgeReceipt {
            kind: "runtime.cross_session_bridge_receipt".to_string(),
            status,
            route,
            command,
            message: bridge_message,
        };
        record_bridge_event(&message, &receipt);
        receipt
    }

    #[must_use]
    pub fn lease_state(session_id: &str, allow_background: bool) -> SessionLeaseState {
        lease_state(session_id, allow_background)
    }
}

fn dispatch_command(
    command: &MissionSessionCommand,
    mode: SessionDispatchMode,
) -> Result<SessionCommandDispatchReceipt, String> {
    let claimed = global_mission_runtime()
        .claim_session_command(&command.target_session_id, &command.command_id)?;
    let completed = match mode {
        SessionDispatchMode::MarkClaimedOnly => claimed,
        SessionDispatchMode::ControlDispatchComplete => global_mission_runtime()
            .complete_session_command(
                &command.target_session_id,
                &command.command_id,
                Some(format!("control-dispatch:{}", command.command_id)),
            )?,
    };
    Ok(SessionCommandDispatchReceipt {
        command_id: command.command_id.clone(),
        session_id: command.target_session_id.clone(),
        status_before: MissionSessionCommandStatus::Pending,
        status_after: completed.status,
        mode,
        message: match mode {
            SessionDispatchMode::MarkClaimedOnly => {
                "command claimed for control-plane dispatch".to_string()
            }
            SessionDispatchMode::ControlDispatchComplete => {
                "command completed by control-plane dispatch".to_string()
            }
        },
    })
}

fn lease_state(session_id: &str, allow_background: bool) -> SessionLeaseState {
    match global_mission_runtime().get_session(session_id) {
        Some(session) => match session.status {
            MissionSessionStatus::Active => SessionLeaseState {
                session_id: session_id.to_string(),
                dispatchable: true,
                reason: "active session".to_string(),
            },
            MissionSessionStatus::Background if allow_background => SessionLeaseState {
                session_id: session_id.to_string(),
                dispatchable: true,
                reason: "background session allowed".to_string(),
            },
            MissionSessionStatus::Background => SessionLeaseState {
                session_id: session_id.to_string(),
                dispatchable: false,
                reason: "background session dispatch disabled".to_string(),
            },
            MissionSessionStatus::Paused => SessionLeaseState {
                session_id: session_id.to_string(),
                dispatchable: false,
                reason: "session paused".to_string(),
            },
            MissionSessionStatus::Closed => SessionLeaseState {
                session_id: session_id.to_string(),
                dispatchable: false,
                reason: "session closed".to_string(),
            },
        },
        None => SessionLeaseState {
            session_id: session_id.to_string(),
            dispatchable: false,
            reason: "session not found".to_string(),
        },
    }
}

fn record_dispatch_event(receipt: &SessionCommandDispatchReceipt) {
    let _ = record_runtime_event(RuntimeEventInput {
        stream_id: format!("session-command:{}", receipt.command_id),
        scope: RuntimeEventScope::SessionCommand,
        kind: "session_execution.dispatched".to_string(),
        status: Some(receipt.status_after.as_str().to_string()),
        actor: Some("session_execution_plane".to_string()),
        refs: vec![RuntimeEventRef {
            kind: "session".to_string(),
            id: receipt.session_id.clone(),
        }],
        payload: serde_json::json!(receipt),
    });
}

fn record_bridge_event(message: &CrossSessionMessage, receipt: &CrossSessionBridgeReceipt) {
    let mut refs = message
        .evidence_refs
        .iter()
        .map(|id| RuntimeEventRef {
            kind: "evidence".to_string(),
            id: id.clone(),
        })
        .collect::<Vec<_>>();
    refs.push(RuntimeEventRef {
        kind: "session".to_string(),
        id: message.from_session_id.clone(),
    });
    let _ = record_runtime_event(RuntimeEventInput {
        stream_id: format!("session:{}", message.from_session_id),
        scope: RuntimeEventScope::SessionCommand,
        kind: "session_execution.bridge".to_string(),
        status: Some(receipt.status.clone()),
        actor: message
            .actor
            .clone()
            .or_else(|| Some("session_execution_plane".to_string())),
        refs,
        payload: serde_json::json!({
            "message": message,
            "receipt": receipt,
        }),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionProxy, StartMissionSessionRequest};

    #[test]
    fn session_execution_dispatches_pending_and_bridges_sessions() {
        let suffix = uuid::Uuid::new_v4();
        let session_a = format!("session-exec-a-{suffix}");
        let session_b = format!("session-exec-b-{suffix}");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "session execution a".to_string(),
                session_id: Some(session_a.clone()),
            })
            .expect("session a");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "session execution b".to_string(),
                session_id: Some(session_b.clone()),
            })
            .expect("session b");
        global_session_relation_graph()
            .upsert_proxy(SessionProxy {
                session_id: session_b.clone(),
                summary: "target proxy".to_string(),
                evidence_refs: Vec::new(),
                decisions: Vec::new(),
                open_questions: Vec::new(),
                updated_at_ms: 1,
            })
            .expect("proxy");

        let bridge = SessionExecutionPlane::bridge(CrossSessionMessage {
            from_session_id: session_a.clone(),
            target_ref: format!("@{session_b}"),
            command: "review bridged work".to_string(),
            actor: Some("test-human".to_string()),
            evidence_refs: vec!["evidence:bridge".to_string()],
        });
        assert_eq!(bridge.status, "routed");
        let command_id = bridge
            .command
            .as_ref()
            .expect("bridged command")
            .command_id
            .clone();

        let report = SessionExecutionPlane::dispatch_pending(SessionExecutionPolicy {
            max_commands: 1_000,
            dispatch_mode: SessionDispatchMode::MarkClaimedOnly,
            allow_background: true,
        });
        let final_status = global_mission_runtime()
            .get_session_command(&command_id)
            .expect("command")
            .status;
        assert!(
            report.dispatched.iter().any(|receipt| {
                receipt.command_id == command_id
                    && receipt.status_after == MissionSessionCommandStatus::Claimed
            }) || final_status != MissionSessionCommandStatus::Pending,
            "bridged command should be claimed by this dispatch report or by a concurrent global dispatcher"
        );
        assert_ne!(final_status, MissionSessionCommandStatus::Pending);
    }
}
