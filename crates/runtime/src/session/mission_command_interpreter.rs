//! Mission command interpreter for model-visible cross-session control.

use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeSpec,
    ExecutionNodeStatus,
};
use harness_contract::turn::{SessionDispatchAction, SessionDispatchCommand, SessionHandoff};
use serde::{Deserialize, Serialize};

use crate::{SessionDispatchMode, SessionExecutionPolicy};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionCommandInterpretRequest {
    pub current_session_id: String,
    pub command_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<String>,
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
        let mut policy = SessionExecutionPolicy::default();
        if let Some(dispatch_mode) = request.dispatch_mode {
            policy.dispatch_mode = dispatch_mode;
        }
        if let Some(allow_background) = request.allow_background {
            policy.allow_background = allow_background;
        }

        if let Some(target_ref) = target_ref.clone() {
            let target_kind = classify_target_ref(target_ref.as_str());
            return match target_kind {
                MissionCommandTargetKind::Session => bridge_interpretation(SessionHandoff {
                    handoff_id: format!("handoff-{}", uuid::Uuid::new_v4()),
                    source_session_id: current_session_id,
                    target_session_id: target_ref.trim_start_matches('@').to_string(),
                    objective: command_text,
                    acceptance: Vec::new(),
                    scope: Vec::new(),
                    context_lens: Vec::new(),
                    evidence_refs: Vec::new(),
                    permission_lease: "session-dispatch-default".to_string(),
                    deadline_at_ms: None,
                    priority: 128,
                    correlation_id: format!("correlation-{}", uuid::Uuid::new_v4()),
                    result_contract: "return_checked_result".to_string(),
                }),
                MissionCommandTargetKind::Agent => blocked(
                    command_text,
                    Some(target_ref),
                    "agent routing must compile an AgentTask; SessionDispatch only accepts SessionHandoff",
                ),
                MissionCommandTargetKind::Team => blocked(
                    command_text,
                    Some(target_ref),
                    "team commands require the scoped TeamRuntime command adapter",
                ),
                MissionCommandTargetKind::Dispatch | MissionCommandTargetKind::Unknown => blocked(
                    command_text,
                    Some(target_ref),
                    "target ref is not actionable",
                ),
            };
        }

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
    pub fn interpret_session_handoff(handoff: SessionHandoff) -> MissionCommandInterpretation {
        Self::interpret_session_handoff_with_action(handoff, SessionDispatchAction::Enqueue)
    }

    /// Compile a typed handoff without introducing a graph-external command
    /// path. MissionCommandRouter selects the action after policy and
    /// revision validation; the executor receives the same durable contract.
    #[must_use]
    pub fn interpret_session_handoff_with_action(
        handoff: SessionHandoff,
        action: SessionDispatchAction,
    ) -> MissionCommandInterpretation {
        bridge_interpretation_with_action(
            handoff,
            action,
            format!("execution-graph-{}", uuid::Uuid::new_v4()),
        )
    }

    /// Compiles a handoff with a stable graph identity owned by an external
    /// durable trigger (for example MissionSchedule). The interpreter remains
    /// side-effect free; stable identity only makes restart submission
    /// idempotent at the GraphRunner boundary.
    #[must_use]
    pub fn interpret_session_handoff_with_graph_id(
        handoff: SessionHandoff,
        graph_id: impl Into<String>,
    ) -> MissionCommandInterpretation {
        bridge_interpretation_with_graph_id(handoff, graph_id.into())
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
                "status": "compiled_graph",
                "side_effects_started": false,
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

fn bridge_interpretation(handoff: SessionHandoff) -> MissionCommandInterpretation {
    bridge_interpretation_with_action(
        handoff,
        SessionDispatchAction::Enqueue,
        format!("execution-graph-{}", uuid::Uuid::new_v4()),
    )
}

fn bridge_interpretation_with_graph_id(
    handoff: SessionHandoff,
    graph_id: String,
) -> MissionCommandInterpretation {
    bridge_interpretation_with_action(handoff, SessionDispatchAction::Enqueue, graph_id)
}

fn bridge_interpretation_with_action(
    handoff: SessionHandoff,
    action: SessionDispatchAction,
    graph_id: String,
) -> MissionCommandInterpretation {
    let target_ref = handoff.target_session_id.clone();
    let command_text = handoff.objective.clone();
    let command = SessionDispatchCommand {
        command_id: format!("session-dispatch-command:{}", handoff.correlation_id),
        action,
        handoff,
        expected_target_revision: 0,
    };
    let payload = serde_json::to_string(&command).unwrap_or_default();
    graph_interpretation(
        command_text,
        Some(target_ref),
        MissionCommandTargetKind::Session,
        session_dispatch_graph_with_id(
            "dispatch typed session handoff",
            format!("session_handoff:{payload}"),
            graph_id,
        ),
        vec![
            "submit SessionDispatch node".to_string(),
            "let Runtime SessionInputRouter validate and enqueue the target".to_string(),
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
    session_dispatch_graph_with_id(
        objective,
        payload_ref,
        format!("execution-graph-{}", uuid::Uuid::new_v4()),
    )
}

fn session_dispatch_graph_with_id(
    objective: impl Into<String>,
    payload_ref: String,
    graph_id: String,
) -> ExecutionGraph {
    let mut graph = ExecutionGraph::new(objective);
    graph.id = graph_id;
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
    } else if target.starts_with("agent-") || target.starts_with("agent:") {
        MissionCommandTargetKind::Agent
    } else {
        MissionCommandTargetKind::Session
    }
}
