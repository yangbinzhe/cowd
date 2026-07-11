//! Mission command interpreter for model-visible cross-session control.

use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeSpec,
    ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};

use crate::{
    CrossSessionMessage, SessionDispatchMode, SessionExecutionPolicy, StewardAutomationPolicy,
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
    SubmitExecutionGraph {
        graph: ExecutionGraph,
        graph_command: ExecutionGraphCommand,
    },
    Blocked {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
                MissionCommandTargetKind::Team => blocked(
                    command_text,
                    Some(target_ref),
                    "team commands require the scoped TeamRuntime command adapter",
                ),
                MissionCommandTargetKind::Steward => MissionCommandInterpretation {
                    kind: "runtime.mission_command_interpretation".to_string(),
                    status: "blocked".to_string(),
                    target_kind,
                    target_ref: Some(target_ref),
                    command_text,
                    command: MissionInterpretedCommand::Blocked {
                        reason: "supervise execution graphs become available in V8".to_string(),
                    },
                    blocked_reason: Some(
                        "supervise execution graphs become available in V8".to_string(),
                    ),
                    execution_plan: vec!["capability_unavailable:supervise:V8".to_string()],
                },
                MissionCommandTargetKind::Dispatch | MissionCommandTargetKind::Unknown => blocked(
                    command_text,
                    Some(target_ref),
                    "target ref is not actionable",
                ),
            };
        }

        let policy = SessionExecutionPolicy {
            max_commands: policy.default_max_session_commands(),
            dispatch_mode,
            allow_background,
        };
        graph_interpretation(
            command_text,
            None,
            MissionCommandTargetKind::Dispatch,
            session_dispatch_graph(
                "dispatch pending mission session inputs",
                format!(
                    "session_dispatch_policy:{}",
                    serde_json::to_string(&policy).unwrap_or_default()
                ),
            ),
            vec![
                "submit SessionDispatch node".to_string(),
                "let Runtime SessionInputRouter materialize commands".to_string(),
            ],
        )
    }

    #[must_use]
    pub fn interpret_session_message(message: CrossSessionMessage) -> MissionCommandInterpretation {
        let target_kind = classify_target_ref(&message.target_ref);
        bridge_interpretation(
            message.from_session_id.clone(),
            message.target_ref.clone(),
            message.command.clone(),
            target_kind,
            target_kind == MissionCommandTargetKind::Session,
        )
    }

    #[must_use]
    pub fn interpret_session_policy(
        policy: SessionExecutionPolicy,
    ) -> MissionCommandInterpretation {
        graph_interpretation(
            "dispatch pending mission session inputs".to_string(),
            None,
            MissionCommandTargetKind::Dispatch,
            session_dispatch_graph(
                "dispatch pending mission session inputs",
                format!(
                    "session_dispatch_policy:{}",
                    serde_json::to_string(&policy).unwrap_or_default()
                ),
            ),
            vec![
                "submit SessionDispatch node".to_string(),
                "let Runtime SessionInputRouter materialize commands".to_string(),
            ],
        )
    }

    /// Prepare a canonical submission for the injected RuntimeHost.
    ///
    /// This method is deliberately side-effect free. Gateway or a background
    /// supervisor must pass the returned graph and command to ExecutionGraphHost.
    pub fn prepare_submission(
        interpretation: MissionCommandInterpretation,
    ) -> MissionCommandExecutionReceipt {
        let result = match &interpretation.command {
            MissionInterpretedCommand::SubmitExecutionGraph {
                graph,
                graph_command,
            } => serde_json::json!({
                "ok": true,
                "status": "pending_runtime_host",
                "graph": graph,
                "command": graph_command,
            }),
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
    let payload = serde_json::to_string(&message).unwrap_or_default();
    graph_interpretation(
        command_text,
        Some(target_ref),
        target_kind,
        session_dispatch_graph(
            if session_target {
                "bridge session input"
            } else {
                "route agent input"
            },
            format!("session_input:{payload}"),
        ),
        vec![
            "submit SessionDispatch node".to_string(),
            "let Runtime SessionInputRouter resolve and enqueue the target".to_string(),
        ],
    )
}

fn graph_interpretation(
    command_text: String,
    target_ref: Option<String>,
    target_kind: MissionCommandTargetKind,
    graph: ExecutionGraph,
    execution_plan: Vec<String>,
) -> MissionCommandInterpretation {
    let expected_revision = graph.revision;
    MissionCommandInterpretation {
        kind: "runtime.mission_command_interpretation".to_string(),
        status: "interpreted".to_string(),
        target_kind,
        target_ref,
        command_text,
        command: MissionInterpretedCommand::SubmitExecutionGraph {
            graph,
            graph_command: ExecutionGraphCommand::Start { expected_revision },
        },
        blocked_reason: None,
        execution_plan,
    }
}

fn session_dispatch_graph(objective: impl Into<String>, payload_ref: String) -> ExecutionGraph {
    let mut graph = ExecutionGraph::new(objective);
    let node = ExecutionNodeSpec::new(
        ExecutionNodeKind::SessionDispatch,
        "session_dispatch",
        payload_ref,
    );
    graph
        .node_statuses
        .insert(node.id.clone(), ExecutionNodeStatus::Planned);
    graph.nodes.push(node);
    graph
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
