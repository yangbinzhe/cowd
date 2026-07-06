//! Mission command interpreter for model-visible cross-session control.

use serde::{Deserialize, Serialize};

use crate::{
    CrossSessionMessage, SessionDispatchMode, SessionExecutionPlane, SessionExecutionPolicy,
    StewardAutomationPolicy, StewardScheduler, StewardSchedulerConfig, TeamExecutionLoop,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionCommandInterpretRequest {
    pub current_session_id: String,
    pub command_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autonomy_policy: Option<StewardAutomationPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_mode: Option<SessionDispatchMode>,
    #[serde(default)]
    pub allow_background: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionCommandInterpretation {
    pub kind: String,
    pub status: String,
    pub target_kind: MissionCommandTargetKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<String>,
    pub command_text: String,
    pub command: MissionInterpretedCommand,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub execution_plan: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionCommandTargetKind {
    Session,
    Agent,
    Team,
    Steward,
    Dispatch,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MissionInterpretedCommand {
    BridgeSession { message: CrossSessionMessage },
    RouteAgent { message: CrossSessionMessage },
    TickTeam { team_id: String },
    TickStewardScheduler { config: StewardSchedulerConfig },
    DispatchPendingSessions { policy: SessionExecutionPolicy },
    Blocked { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionCommandExecutionReceipt {
    pub kind: String,
    pub ok: bool,
    pub interpretation: MissionCommandInterpretation,
    pub result: serde_json::Value,
}

pub struct MissionCommandInterpreter;

impl MissionCommandInterpreter {
    #[must_use]
    pub fn interpret(request: MissionCommandInterpretRequest) -> MissionCommandInterpretation {
        let current_session_id = request.current_session_id.trim().to_string();
        let command_text = request.command_text.trim().to_string();
        if current_session_id.is_empty() {
            return blocked(command_text, None, "current_session_id must not be empty");
        }
        if command_text.is_empty() {
            return blocked(command_text, None, "command_text must not be empty");
        }
        let target_ref = request
            .target_ref
            .clone()
            .or_else(|| extract_target_ref(command_text.as_str()));
        let policy = request.autonomy_policy.unwrap_or_default();
        let dispatch_mode = request
            .dispatch_mode
            .unwrap_or_else(|| policy.default_dispatch_mode());
        let allow_background = request.allow_background.unwrap_or(true);

        if let Some(target_ref) = target_ref.clone() {
            let target_kind = classify_target_ref(target_ref.as_str());
            return match target_kind {
                MissionCommandTargetKind::Session => bridge_interpretation(
                    current_session_id,
                    target_ref,
                    command_text,
                    MissionCommandTargetKind::Session,
                    true,
                ),
                MissionCommandTargetKind::Agent => bridge_interpretation(
                    current_session_id,
                    target_ref,
                    command_text,
                    MissionCommandTargetKind::Agent,
                    false,
                ),
                MissionCommandTargetKind::Team => {
                    let team_id = target_ref.trim_start_matches('@').to_string();
                    MissionCommandInterpretation {
                        kind: "runtime.mission_command_interpretation".to_string(),
                        status: "interpreted".to_string(),
                        target_kind,
                        target_ref: Some(target_ref),
                        command_text,
                        command: MissionInterpretedCommand::TickTeam { team_id },
                        blocked_reason: None,
                        execution_plan: vec![
                            "plan team workgraph".to_string(),
                            "tick ready agent tasks".to_string(),
                            "collect team execution report".to_string(),
                        ],
                    }
                }
                MissionCommandTargetKind::Steward => MissionCommandInterpretation {
                    kind: "runtime.mission_command_interpretation".to_string(),
                    status: "interpreted".to_string(),
                    target_kind,
                    target_ref: Some(target_ref),
                    command_text,
                    command: MissionInterpretedCommand::TickStewardScheduler {
                        config: StewardSchedulerConfig {
                            policy,
                            dispatch_mode,
                            allow_background_sessions: allow_background,
                            ..StewardSchedulerConfig::default()
                        },
                    },
                    blocked_reason: None,
                    execution_plan: vec![
                        "tick steward runtime".to_string(),
                        "dispatch session commands by policy".to_string(),
                        "tick team workgraphs".to_string(),
                    ],
                },
                MissionCommandTargetKind::Dispatch | MissionCommandTargetKind::Unknown => blocked(
                    command_text,
                    Some(target_ref),
                    "target ref is not actionable",
                ),
            };
        }

        MissionCommandInterpretation {
            kind: "runtime.mission_command_interpretation".to_string(),
            status: "interpreted".to_string(),
            target_kind: MissionCommandTargetKind::Dispatch,
            target_ref: None,
            command_text,
            command: MissionInterpretedCommand::DispatchPendingSessions {
                policy: SessionExecutionPolicy {
                    max_commands: policy.default_max_session_commands(),
                    dispatch_mode,
                    allow_background,
                },
            },
            blocked_reason: None,
            execution_plan: vec![
                "inspect pending mission session commands".to_string(),
                "apply lease/background policy".to_string(),
                "emit turn requests or control-plane receipts".to_string(),
            ],
        }
    }

    pub fn execute(interpretation: MissionCommandInterpretation) -> MissionCommandExecutionReceipt {
        let result = match &interpretation.command {
            MissionInterpretedCommand::BridgeSession { message }
            | MissionInterpretedCommand::RouteAgent { message } => {
                serde_json::json!(SessionExecutionPlane::bridge(message.clone()))
            }
            MissionInterpretedCommand::TickTeam { team_id } => {
                match TeamExecutionLoop::tick_ready(team_id) {
                    Ok(report) => {
                        serde_json::json!({ "ok": report.errors.is_empty(), "report": report })
                    }
                    Err(error) => serde_json::json!({ "ok": false, "error": error }),
                }
            }
            MissionInterpretedCommand::TickStewardScheduler { config } => {
                serde_json::json!(StewardScheduler::tick(config.clone()))
            }
            MissionInterpretedCommand::DispatchPendingSessions { policy } => {
                serde_json::json!(SessionExecutionPlane::dispatch_pending(policy.clone()))
            }
            MissionInterpretedCommand::Blocked { reason } => {
                serde_json::json!({ "ok": false, "error": reason })
            }
        };
        let ok = result
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or_else(|| {
                result
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|status| status == "routed" || status == "interpreted")
            });
        MissionCommandExecutionReceipt {
            kind: "runtime.mission_command_execution_receipt".to_string(),
            ok,
            interpretation,
            result,
        }
    }
}

fn bridge_interpretation(
    current_session_id: String,
    target_ref: String,
    command_text: String,
    target_kind: MissionCommandTargetKind,
    session_target: bool,
) -> MissionCommandInterpretation {
    let message = CrossSessionMessage {
        from_session_id: current_session_id,
        target_ref: target_ref.clone(),
        command: command_text.clone(),
        actor: Some("mission_command_interpreter".to_string()),
        evidence_refs: Vec::new(),
    };
    MissionCommandInterpretation {
        kind: "runtime.mission_command_interpretation".to_string(),
        status: "interpreted".to_string(),
        target_kind,
        target_ref: Some(target_ref),
        command_text,
        command: if session_target {
            MissionInterpretedCommand::BridgeSession { message }
        } else {
            MissionInterpretedCommand::RouteAgent { message }
        },
        blocked_reason: None,
        execution_plan: if session_target {
            vec![
                "resolve session proxy or relation target".to_string(),
                "enqueue command into target session inbox".to_string(),
                "dispatch according to steward/session policy".to_string(),
            ]
        } else {
            vec![
                "resolve agent lifecycle and team binding".to_string(),
                "deliver runtime input if backend supports it".to_string(),
                "surface recovery candidate if agent is unavailable".to_string(),
            ]
        },
    }
}

fn blocked(
    command_text: String,
    target_ref: Option<String>,
    reason: impl Into<String>,
) -> MissionCommandInterpretation {
    let reason = reason.into();
    MissionCommandInterpretation {
        kind: "runtime.mission_command_interpretation".to_string(),
        status: "blocked".to_string(),
        target_kind: MissionCommandTargetKind::Unknown,
        target_ref,
        command_text,
        command: MissionInterpretedCommand::Blocked {
            reason: reason.clone(),
        },
        blocked_reason: Some(reason),
        execution_plan: vec!["surface blocked reason to controller".to_string()],
    }
}

fn extract_target_ref(command_text: &str) -> Option<String> {
    command_text
        .split_whitespace()
        .find(|token| token.starts_with('@') && token.len() > 1)
        .map(|token| {
            token
                .trim_matches(|ch: char| ch == ',' || ch == ';' || ch == ':' || ch == '.')
                .to_string()
        })
}

fn classify_target_ref(target_ref: &str) -> MissionCommandTargetKind {
    let target = target_ref.trim_start_matches('@').to_ascii_lowercase();
    if target.starts_with("team-") || target.starts_with("team_") || target.starts_with("team:") {
        MissionCommandTargetKind::Team
    } else if target.starts_with("steward-") || target.starts_with("steward:") {
        MissionCommandTargetKind::Steward
    } else if target.starts_with("agent-") || target.starts_with("agent:") {
        MissionCommandTargetKind::Agent
    } else {
        MissionCommandTargetKind::Session
    }
}
