//! Multi-session execution plane.
//!
//! This module gives Mission Runtime an explicit dispatcher and cross-session
//! bridge. It owns leases and command dispatch receipts, while service adapters
//! own the concrete provider turn execution.

use serde::{Deserialize, Serialize};

use crate::{
    global_agent_lifecycle_service, global_agent_task_binding_registry, global_mission_runtime,
    global_session_relation_graph, record_runtime_event, AgentExecutionCommandKind,
    AgentExecutionCommandReceipt, MissionSessionCommand, MissionSessionCommandStatus,
    MissionSessionStatus, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope,
    SessionRouteCommand, SessionRouteReceipt,
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
    StartRuntimeTurn,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_request: Option<SessionTurnDispatchRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTurnDispatchRequest {
    pub session_id: String,
    pub command_id: String,
    pub prompt: String,
    pub source: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_command: Option<AgentExecutionCommandReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_candidate: Option<SessionRecoveryCandidate>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecoveryCandidate {
    pub scope: String,
    pub session_id: Option<String>,
    pub command_id: Option<String>,
    pub agent_id: Option<String>,
    pub status: String,
    pub reason: String,
    pub suggested_action: String,
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
        let (status, command, agent_command, recovery_candidate, bridge_message) =
            if let Some(target_session_id) = route.resolved_session_id.clone() {
                match global_mission_runtime().enqueue_session_command(
                    &message.from_session_id,
                    &target_session_id,
                    message.command.clone(),
                ) {
                    Ok(command) => (
                        "routed".to_string(),
                        Some(command),
                        None,
                        None,
                        "cross-session command enqueued".to_string(),
                    ),
                    Err(error) => ("failed".to_string(), None, None, None, error),
                }
            } else if route.resolved_agent_id.is_some() {
                route_agent_command(&message, &route)
            } else {
                (
                    "rejected".to_string(),
                    None,
                    None,
                    None,
                    route.message.clone(),
                )
            };
        let receipt = CrossSessionBridgeReceipt {
            kind: "runtime.cross_session_bridge_receipt".to_string(),
            status,
            route,
            command,
            agent_command,
            recovery_candidate,
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

fn route_agent_command(
    message: &CrossSessionMessage,
    route: &SessionRouteReceipt,
) -> (
    String,
    Option<MissionSessionCommand>,
    Option<AgentExecutionCommandReceipt>,
    Option<SessionRecoveryCandidate>,
    String,
) {
    let Some(agent_id) = route.resolved_agent_id.as_deref() else {
        return (
            "rejected".to_string(),
            None,
            None,
            None,
            "agent route missing resolved agent id".to_string(),
        );
    };
    let binding = global_agent_task_binding_registry().get_by_agent(agent_id);
    let Some(capability) = global_agent_lifecycle_service().command_capability(agent_id) else {
        return (
            "blocked_missing_agent_binding".to_string(),
            None,
            None,
            Some(SessionRecoveryCandidate {
                scope: "agent".to_string(),
                session_id: Some(message.from_session_id.clone()),
                command_id: None,
                agent_id: Some(agent_id.to_string()),
                status: "blocked_missing_agent_binding".to_string(),
                reason: format!("agent {agent_id} is not active in lifecycle registry"),
                suggested_action: "inspect team binding or restart delegated agent".to_string(),
            }),
            format!("agent target {agent_id} has no active lifecycle binding"),
        );
    };
    if !capability.supports_input {
        return (
            "blocked_agent_input_unavailable".to_string(),
            None,
            None,
            Some(SessionRecoveryCandidate {
                scope: "agent".to_string(),
                session_id: Some(message.from_session_id.clone()),
                command_id: None,
                agent_id: Some(agent_id.to_string()),
                status: "blocked_agent_input_unavailable".to_string(),
                reason: format!("agent backend {} does not accept runtime input", capability.mode),
                suggested_action: "handoff through team task outcome or restart with process-jsonl command channel".to_string(),
            }),
            format!("agent {agent_id} backend does not accept runtime input"),
        );
    }
    match global_agent_lifecycle_service().command(
        agent_id,
        AgentExecutionCommandKind::Input,
        Some(serde_json::json!({
            "from_session_id": message.from_session_id,
            "target_ref": message.target_ref,
            "text": message.command,
            "team_binding": binding,
        })),
    ) {
        Ok(receipt) => (
            "routed".to_string(),
            None,
            Some(receipt),
            None,
            "cross-session command delivered to agent runtime".to_string(),
        ),
        Err(error) => (
            "blocked_agent_command_failed".to_string(),
            None,
            None,
            Some(SessionRecoveryCandidate {
                scope: "agent".to_string(),
                session_id: Some(message.from_session_id.clone()),
                command_id: None,
                agent_id: Some(agent_id.to_string()),
                status: "blocked_agent_command_failed".to_string(),
                reason: error.clone(),
                suggested_action: "retry, inspect agent lifecycle, or takeover manually"
                    .to_string(),
            }),
            error,
        ),
    }
}

fn dispatch_command(
    command: &MissionSessionCommand,
    mode: SessionDispatchMode,
) -> Result<SessionCommandDispatchReceipt, String> {
    let status_before = command.status;
    let claimed = match mode {
        SessionDispatchMode::StartRuntimeTurn => global_mission_runtime()
            .mark_session_command_running(&command.target_session_id, &command.command_id)?,
        SessionDispatchMode::MarkClaimedOnly | SessionDispatchMode::ControlDispatchComplete => {
            global_mission_runtime()
                .claim_session_command(&command.target_session_id, &command.command_id)?
        }
    };
    let completed = match mode {
        SessionDispatchMode::MarkClaimedOnly => claimed,
        SessionDispatchMode::StartRuntimeTurn => claimed,
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
        status_before,
        status_after: completed.status,
        mode,
        message: match mode {
            SessionDispatchMode::MarkClaimedOnly => {
                "command claimed for control-plane dispatch".to_string()
            }
            SessionDispatchMode::ControlDispatchComplete => {
                "command completed by control-plane dispatch".to_string()
            }
            SessionDispatchMode::StartRuntimeTurn => {
                "command marked running for runtime turn dispatch".to_string()
            }
        },
        turn_request: (mode == SessionDispatchMode::StartRuntimeTurn).then(|| {
            SessionTurnDispatchRequest {
                session_id: command.target_session_id.clone(),
                command_id: command.command_id.clone(),
                prompt: command.command.clone(),
                source: "session_execution_plane".to_string(),
            }
        }),
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
    use crate::{
        MissionCommandInterpretRequest, MissionCommandInterpreter, MissionInterpretedCommand,
        SessionProxy, StartMissionSessionRequest,
    };

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

    #[test]
    fn session_execution_start_runtime_turn_marks_running_and_returns_turn_request() {
        let suffix = uuid::Uuid::new_v4();
        let session_a = format!("session-turn-a-{suffix}");
        let session_b = format!("session-turn-b-{suffix}");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "session turn a".to_string(),
                session_id: Some(session_a.clone()),
            })
            .expect("session a");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "session turn b".to_string(),
                session_id: Some(session_b.clone()),
            })
            .expect("session b");
        let command = global_mission_runtime()
            .enqueue_session_command(&session_a, &session_b, "analyze background task")
            .expect("command");

        let report = SessionExecutionPlane::dispatch_pending(SessionExecutionPolicy {
            max_commands: 1_000,
            dispatch_mode: SessionDispatchMode::StartRuntimeTurn,
            allow_background: true,
        });

        let Some(receipt) = report
            .dispatched
            .iter()
            .find(|receipt| receipt.command_id == command.command_id)
        else {
            let final_status = global_mission_runtime()
                .get_session_command(&command.command_id)
                .expect("command")
                .status;
            assert_ne!(
                final_status,
                MissionSessionCommandStatus::Pending,
                "test command should be handled by this report or by a concurrent global dispatcher"
            );
            return;
        };
        assert_eq!(receipt.status_before, MissionSessionCommandStatus::Pending);
        assert_eq!(receipt.status_after, MissionSessionCommandStatus::Running);
        let turn_request = receipt.turn_request.as_ref().expect("turn request");
        assert_eq!(turn_request.session_id, session_b);
        assert_eq!(turn_request.command_id, command.command_id);
        assert_eq!(turn_request.prompt, "analyze background task");
    }

    #[test]
    fn bridge_agent_target_returns_runtime_block_instead_of_deferred_placeholder() {
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("session-agent-bridge-{suffix}");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "agent bridge source".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("session");

        let receipt = SessionExecutionPlane::bridge(CrossSessionMessage {
            from_session_id: session_id,
            target_ref: format!("@agent-missing-{suffix}"),
            command: "inspect current task".to_string(),
            actor: Some("test".to_string()),
            evidence_refs: Vec::new(),
        });

        assert_eq!(receipt.status, "blocked_missing_agent_binding");
        assert!(!receipt.message.contains("TeamExecutionLoop stage"));
        assert!(receipt.recovery_candidate.is_some());
    }

    #[test]
    fn mission_command_interpreter_executes_session_bridge_without_gateway_logic() {
        let suffix = uuid::Uuid::new_v4();
        let session_a = format!("session-interpreter-a-{suffix}");
        let session_b = format!("session-interpreter-b-{suffix}");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "interpreter source".to_string(),
                session_id: Some(session_a.clone()),
            })
            .expect("session a");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "interpreter target".to_string(),
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

        let interpretation = MissionCommandInterpreter::interpret(MissionCommandInterpretRequest {
            current_session_id: session_a,
            command_text: format!("@{session_b} review this branch"),
            target_ref: None,
            autonomy_policy: None,
            dispatch_mode: None,
            allow_background: Some(true),
        });
        assert_eq!(interpretation.status, "interpreted");
        assert!(matches!(
            interpretation.command,
            MissionInterpretedCommand::BridgeSession { .. }
        ));
        let receipt = MissionCommandInterpreter::execute(interpretation);
        assert!(receipt.ok);
        assert!(receipt.result["command"].is_object());
    }

    #[test]
    fn recovery_marks_running_interrupted_and_claimed_pending_not_completed() {
        let suffix = uuid::Uuid::new_v4();
        let session_a = format!("session-recovery-a-{suffix}");
        let session_b = format!("session-recovery-b-{suffix}");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "recovery source".to_string(),
                session_id: Some(session_a.clone()),
            })
            .expect("session a");
        global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "recovery target".to_string(),
                session_id: Some(session_b.clone()),
            })
            .expect("session b");
        let claimed = global_mission_runtime()
            .enqueue_session_command(&session_a, &session_b, "claimed command")
            .expect("claimed");
        let running = global_mission_runtime()
            .enqueue_session_command(&session_a, &session_b, "running command")
            .expect("running");
        global_mission_runtime()
            .claim_session_command(&session_b, &claimed.command_id)
            .expect("claim");
        global_mission_runtime()
            .mark_session_command_running(&session_b, &running.command_id)
            .expect("running");

        let report = global_mission_runtime().recover_interrupted_work();
        assert!(report
            .recovered
            .iter()
            .any(|candidate| candidate.command_id.as_deref() == Some(claimed.command_id.as_str())));
        assert!(report
            .recovered
            .iter()
            .any(|candidate| candidate.command_id.as_deref() == Some(running.command_id.as_str())));
        assert_eq!(
            global_mission_runtime()
                .get_session_command(&claimed.command_id)
                .expect("claimed after")
                .status,
            MissionSessionCommandStatus::Pending
        );
        assert_eq!(
            global_mission_runtime()
                .get_session_command(&running.command_id)
                .expect("running after")
                .status,
            MissionSessionCommandStatus::Interrupted
        );
    }
}
